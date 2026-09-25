use std::{io, thread, time::Duration};

use actix_rt::time::Instant;
use mio::{Interest, Poll, Token as MioToken};
use tracing::{debug, error, info};

use crate::{
    availability::Availability,
    socket::MioListener,
    waker_queue::{WakerInterest, WakerQueue, WAKER_TOKEN},
    worker::{Conn, ServerWorker, WorkerHandleAccept, WorkerHandleServer},
    ServerBuilder, ServerHandle,
};

const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(500);
// Allow a 10 ms margin after the listener's retry deadline.
const ACCEPT_ERROR_POLL_TIMEOUT: Duration =
    ACCEPT_ERROR_BACKOFF.saturating_add(Duration::from_millis(10));

struct ServerSocketInfo {
    token: usize,

    lst: MioListener,

    /// Timeout is used to mark the deadline when this socket's listener should be registered again
    /// after an error.
    timeout: Option<actix_rt::time::Instant>,
}

/// Accepts connections from listeners and dispatches them to workers.
pub(crate) struct Accept {
    /// Waits for listener readiness and notifications from the waker queue.
    poll: Poll,

    /// Shared queue for server commands, worker availability, and replacement worker handles.
    waker_queue: WakerQueue,

    /// Worker handles used to send accepted connections and update connection counts.
    handles: Vec<WorkerHandleAccept>,

    /// Notifies the server when a worker fails so it can start a replacement.
    srv: ServerHandle,

    /// Index into `handles` of the next worker to consider for connection dispatch.
    /// Advances after a successful dispatch or when an unavailable worker is skipped.
    next_worker: usize,

    /// Index into the listener slice at which the next `accept_all` scan starts.
    /// Rotates between scans so listeners take turns using newly available worker capacity.
    next_listener: usize,

    /// Cached connection capacity flags, indexed by worker ID rather than position in `handles`.
    avail: Availability,

    /// Poll timeout used to retry listeners after accept errors, taking the shortest requested wait.
    /// `None` lets the poll wait indefinitely for an event.
    timeout: Option<Duration>,

    /// True if accepting connections is paused by a server command.
    /// Worker availability notifications do not resume acceptance while this is set.
    paused: bool,
}

impl Accept {
    pub(crate) fn start(
        sockets: Vec<(usize, MioListener)>,
        builder: &ServerBuilder,
    ) -> io::Result<(WakerQueue, Vec<WorkerHandleServer>, thread::JoinHandle<()>)> {
        let handle_server = ServerHandle::new(builder.cmd_tx.clone());

        // construct poll instance and its waker
        let poll = Poll::new()?;
        let waker_queue = WakerQueue::new(poll.registry())?;

        // start workers and collect handles
        let (handles_accept, handles_server) = (0..builder.threads)
            .map(|idx| {
                // clone service factories
                let factories = builder
                    .factories
                    .iter()
                    .map(|f| f.clone_factory())
                    .collect::<Vec<_>>();

                // start worker using service factories
                ServerWorker::start(idx, factories, waker_queue.clone(), builder.worker_config)
            })
            .collect::<io::Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        let (mut accept, mut sockets) = Accept::new_with_sockets(
            poll,
            waker_queue.clone(),
            sockets,
            handles_accept,
            handle_server,
        )?;

        let accept_handle = thread::Builder::new()
            .name("actix-server acceptor".to_owned())
            .spawn(move || accept.poll_with(&mut sockets))
            .map_err(io::Error::other)?;

        Ok((waker_queue, handles_server, accept_handle))
    }

    fn new_with_sockets(
        poll: Poll,
        waker_queue: WakerQueue,
        sockets: Vec<(usize, MioListener)>,
        accept_handles: Vec<WorkerHandleAccept>,
        server_handle: ServerHandle,
    ) -> io::Result<(Accept, Box<[ServerSocketInfo]>)> {
        let sockets = sockets
            .into_iter()
            .map(|(token, mut lst)| {
                // Start listening for incoming connections
                poll.registry()
                    .register(&mut lst, MioToken(token), Interest::READABLE)?;

                Ok(ServerSocketInfo {
                    token,
                    lst,
                    timeout: None,
                })
            })
            .collect::<io::Result<_>>()?;

        let mut avail = Availability::default();

        // Assume all handles are avail at construct time.
        avail.set_available_all(&accept_handles);

        let accept = Accept {
            poll,
            waker_queue,
            handles: accept_handles,
            srv: server_handle,
            next_worker: 0,
            next_listener: 0,
            avail,
            timeout: None,
            paused: false,
        };

        Ok((accept, sockets))
    }

    /// blocking wait for readiness events triggered by mio
    fn poll_with(&mut self, sockets: &mut [ServerSocketInfo]) {
        let mut events = mio::Events::with_capacity(256);

        loop {
            if let Err(err) = self.poll.poll(&mut events, self.timeout) {
                match err.kind() {
                    io::ErrorKind::Interrupted => {}
                    _ => panic!("Poll error: {err}"),
                }
            }

            for event in events.iter() {
                let token = event.token();
                match token {
                    WAKER_TOKEN => {
                        let exit = self.handle_waker(sockets);
                        if exit {
                            info!("accept thread stopped");
                            return;
                        }
                    }
                    _ => {
                        let token = usize::from(token);
                        self.accept(sockets, token);
                    }
                }
            }

            // check for timeout and re-register sockets
            self.process_timeout(sockets);
        }
    }

    fn handle_waker(&mut self, sockets: &mut [ServerSocketInfo]) -> bool {
        // This is a loop because interests for command from previous version was
        // a loop that would try to drain the command channel. It's yet unknown
        // if it's necessary/good practice to actively drain the waker queue.
        loop {
            // Take guard with every iteration so no new interests can be added until the current
            // task is done. Take care not to take the guard again inside this loop.
            let mut guard = self.waker_queue.guard();

            match guard.pop_front() {
                // Worker notified it became available.
                Some(WakerInterest::WorkerAvailable(idx)) => {
                    drop(guard);

                    self.avail.set_available(idx, true);

                    if !self.paused {
                        self.accept_all(sockets);
                    }
                }

                // A new worker thread has been created so store its handle.
                Some(WakerInterest::Worker(handle)) => {
                    drop(guard);

                    self.avail.set_available(handle.idx(), true);
                    self.handles.push(handle);

                    if !self.paused {
                        self.accept_all(sockets);
                    }
                }

                Some(WakerInterest::Pause) => {
                    drop(guard);

                    if !self.paused {
                        self.paused = true;

                        self.deregister_all(sockets);
                    }
                }

                Some(WakerInterest::Resume) => {
                    drop(guard);

                    if self.paused {
                        self.paused = false;

                        sockets.iter_mut().for_each(|info| {
                            self.register_logged(info);
                        });

                        self.accept_all(sockets);
                    }
                }

                Some(WakerInterest::Stop) => {
                    if !self.paused {
                        self.deregister_all(sockets);
                    }

                    return true;
                }

                // waker queue is drained
                None => {
                    // Reset the WakerQueue before break so it does not grow infinitely
                    WakerQueue::reset(&mut guard);

                    return false;
                }
            }
        }
    }

    fn process_timeout(&mut self, sockets: &mut [ServerSocketInfo]) {
        // always remove old timeouts
        if self.timeout.take().is_some() {
            let now = Instant::now();

            sockets
                .iter_mut()
                // Only sockets that had an associated timeout were deregistered.
                .filter(|info| info.timeout.is_some())
                .for_each(|info| {
                    let inst = info.timeout.take().unwrap();

                    if now < inst {
                        // still timed out; try to set new timeout
                        info.timeout = Some(inst);
                        self.set_timeout(inst - now);
                    } else if !self.paused {
                        // timeout expired; register socket again
                        self.register_logged(info);
                    }

                    // Drop the timeout if server is paused and socket timeout is expired.
                    // When server recovers from pause it will register all sockets without
                    // a timeout value so this socket register will be delayed till then.
                });
        }
    }

    /// Update accept timeout with `duration` if it is shorter than current timeout.
    fn set_timeout(&mut self, duration: Duration) {
        match self.timeout {
            Some(ref mut timeout) => {
                if *timeout > duration {
                    *timeout = duration;
                }
            }
            None => self.timeout = Some(duration),
        }
    }

    #[cfg(not(target_os = "windows"))]
    fn register(&self, info: &mut ServerSocketInfo) -> io::Result<()> {
        let token = MioToken(info.token);
        self.poll
            .registry()
            .register(&mut info.lst, token, Interest::READABLE)
    }

    #[cfg(target_os = "windows")]
    fn register(&self, info: &mut ServerSocketInfo) -> io::Result<()> {
        // On windows, calling register without deregister cause an error.
        // See https://github.com/actix/actix-web/issues/905
        // Calling reregister seems to fix the issue.
        let token = MioToken(info.token);
        self.poll
            .registry()
            .register(&mut info.lst, token, Interest::READABLE)
            .or_else(|_| {
                self.poll
                    .registry()
                    .reregister(&mut info.lst, token, Interest::READABLE)
            })
    }

    fn register_logged(&self, info: &mut ServerSocketInfo) {
        match self.register(info) {
            Ok(_) => debug!("resume accepting connections on {}", info.lst.local_addr()),
            Err(err) => error!("can not register server socket {}", err),
        }
    }

    fn deregister_logged(&self, info: &mut ServerSocketInfo) {
        match self.poll.registry().deregister(&mut info.lst) {
            Ok(_) => debug!("paused accepting connections on {}", info.lst.local_addr()),
            Err(err) => {
                error!("can not deregister server socket {}", err)
            }
        }
    }

    fn deregister_all(&self, sockets: &mut [ServerSocketInfo]) {
        // This is a best effort implementation with following limitation:
        //
        // Every ServerSocketInfo with associated timeout will be skipped and it's timeout is
        // removed in the process.
        //
        // Therefore WakerInterest::Pause followed by WakerInterest::Resume in a very short gap
        // (less than 500ms) would cause all timing out ServerSocketInfos be re-registered before
        // expected timing.
        sockets
            .iter_mut()
            // Take all timeout.
            // This is to prevent Accept::process_timer method re-register a socket afterwards.
            .map(|info| (info.timeout.take(), info))
            // Socket info with a timeout is already deregistered so skip them.
            .filter(|(timeout, _)| timeout.is_none())
            .for_each(|(_, info)| self.deregister_logged(info));
    }

    /// Sends connection to worker and handles error.
    fn send_connection(&mut self, conn: Conn) -> Result<(), Conn> {
        let next = self.next();

        // Count the connection before the worker can receive it, so a worker handling a stop
        // command never holds a connection that is missing from its count.
        let below_limit = next.inc_counter();

        match next.send(conn) {
            Ok(_) => {
                // Set worker to unavailable when it hit max.
                if !below_limit {
                    let idx = next.idx();
                    self.avail.set_available(idx, false);
                }
                self.set_next();
                Ok(())
            }
            Err(conn) => {
                // The worker never received the connection.
                next.dec_counter();

                // Worker thread is error and could be gone.
                // Remove worker handle and notify `ServerBuilder`.
                self.remove_next();

                if self.handles.is_empty() {
                    error!("no workers");
                    // All workers are gone and Conn is nowhere to be sent.
                    // Treat this situation as Ok and drop Conn.
                    return Ok(());
                } else if self.handles.len() <= self.next_worker {
                    self.next_worker = 0;
                }

                Err(conn)
            }
        }
    }

    fn accept_one(&mut self, mut conn: Conn) {
        loop {
            let next = self.next();
            let idx = next.idx();

            if self.avail.get_available(idx) {
                match self.send_connection(conn) {
                    Ok(_) => return,
                    Err(c) => conn = c,
                }
            } else {
                self.avail.set_available(idx, false);
                self.set_next();

                if !self.avail.available() {
                    while let Err(c) = self.send_connection(conn) {
                        conn = c;
                    }
                    return;
                }
            }
        }
    }

    fn accept(&mut self, sockets: &mut [ServerSocketInfo], token: usize) {
        while self.avail.available() {
            let info = &mut sockets[token];

            match info.lst.accept() {
                Ok(io) => {
                    let conn = Conn { io, token };
                    self.accept_one(conn);
                }
                Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => return,
                Err(ref err) if connection_error(err) => continue,
                Err(err) => {
                    error!("error accepting connection: {}", err);

                    // deregister listener temporary
                    self.deregister_logged(info);

                    // sleep after error. write the timeout to socket info as later
                    // the poll would need it mark which socket and when it's
                    // listener should be registered
                    info.timeout = Some(Instant::now() + ACCEPT_ERROR_BACKOFF);
                    self.set_timeout(ACCEPT_ERROR_POLL_TIMEOUT);

                    return;
                }
            };
        }
    }

    /// Checks every listener for connections, starting at `next_listener` and wrapping around.
    ///
    /// Each listener accepts connections until worker capacity is exhausted, no connection is
    /// ready, or a listener error stops acceptance. The first listener rotates between calls so a
    /// busy listener cannot always take newly available capacity before the other listeners.
    /// Rotation applies to whole scans, not to individual connections within a scan.
    ///
    /// The listener slice must be nonempty, as required when the server starts.
    fn accept_all(&mut self, sockets: &mut [ServerSocketInfo]) {
        if sockets.len() == 1 {
            self.accept(sockets, sockets[0].token);
            return;
        }

        let start = self.next_listener;
        for offset in 0..sockets.len() {
            let idx = (start + offset) % sockets.len();
            self.accept(sockets, sockets[idx].token);
        }

        // Let each listener use newly available capacity before the others.
        self.next_listener = (start + 1) % sockets.len();
    }

    #[inline(always)]
    fn next(&self) -> &WorkerHandleAccept {
        &self.handles[self.next_worker]
    }

    /// Set next worker handle that would accept connection.
    #[inline(always)]
    fn set_next(&mut self) {
        self.next_worker = (self.next_worker + 1) % self.handles.len();
    }

    /// Remove next worker handle that fail to accept connection.
    fn remove_next(&mut self) {
        let handle = self.handles.swap_remove(self.next_worker);
        let idx = handle.idx();
        // A message is sent to `ServerBuilder` future to notify it a new worker
        // should be made.
        self.srv.worker_faulted(idx);
        self.avail.set_available(idx, false);
    }
}

/// This function defines errors that are per-connection; if we get this error from the `accept()`
/// system call it means the next connection might be ready to be accepted.
///
/// All other errors will incur a timeout before next `accept()` call is attempted. The timeout is
/// useful to handle resource exhaustion errors like `ENFILE` and `EMFILE`. Otherwise, it could
/// enter into a temporary spin loop.
fn connection_error(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::ConnectionRefused
        || err.kind() == io::ErrorKind::ConnectionAborted
        || err.kind() == io::ErrorKind::ConnectionReset
}

#[cfg(test)]
mod tests {
    use std::{
        net,
        sync::{Arc, Mutex},
        task::{Context, Wake, Waker},
    };

    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

    use super::*;
    use crate::{
        socket::MioStream,
        worker::{handle_pair, Counter},
    };

    struct CountOnWake {
        counter: Counter,
        observed: Mutex<Vec<usize>>,
    }

    impl Wake for CountOnWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.observed.lock().unwrap().push(self.counter.total());
        }
    }

    fn connection() -> Conn {
        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().unwrap();

        Conn {
            io: MioStream::Tcp(mio::net::TcpStream::from_std(stream)),
            token: 0,
        }
    }

    fn worker(idx: usize) -> (WorkerHandleAccept, UnboundedReceiver<Conn>, Counter) {
        let (conn_tx, conn_rx) = unbounded_channel();
        let (stop_tx, _) = unbounded_channel();
        let counter = Counter::new(25600);
        let (handle, _) = handle_pair(idx, conn_tx, stop_tx, counter.clone());

        (handle, conn_rx, counter)
    }

    fn accept(handles: Vec<WorkerHandleAccept>) -> Accept {
        let poll = Poll::new().unwrap();
        let waker_queue = WakerQueue::new(poll.registry()).unwrap();
        let (cmd_tx, _) = unbounded_channel();

        let (accept, _) = Accept::new_with_sockets(
            poll,
            waker_queue,
            Vec::new(),
            handles,
            ServerHandle::new(cmd_tx),
        )
        .unwrap();

        accept
    }

    #[test]
    fn connection_is_counted_before_worker_can_receive_it() {
        let (handle, mut conn_rx, counter) = worker(0);
        let mut accept = accept(vec![handle]);

        let recorder = Arc::new(CountOnWake {
            counter,
            observed: Mutex::new(Vec::new()),
        });
        let waker = Waker::from(Arc::clone(&recorder));
        assert!(conn_rx
            .poll_recv(&mut Context::from_waker(&waker))
            .is_pending());

        accept.send_connection(connection()).unwrap();

        assert_eq!(*recorder.observed.lock().unwrap(), [1]);
    }

    #[test]
    fn connection_count_is_restored_when_worker_is_gone() {
        let (gone, gone_rx, gone_counter) = worker(0);
        let (live, mut live_rx, live_counter) = worker(1);
        drop(gone_rx);
        let mut accept = accept(vec![gone, live]);

        let conn = accept.send_connection(connection()).unwrap_err();
        assert_eq!(gone_counter.total(), 0);

        accept.send_connection(conn).unwrap();
        assert_eq!(live_counter.total(), 1);
        assert!(live_rx.try_recv().is_ok());
    }
}
