use intrusive_collections::{intrusive_adapter, UnsafeRef, XorLinkedList, XorLinkedListLink};
use io_uring::squeue::Entry;
use io_uring::IoUring;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

enum Inner {
    Pending(Entry),
    Submitted,
    Completed(i32),
}

pub struct IoFuture {
    state: Inner,
    waker: Option<Waker>,
    link: XorLinkedListLink,
}

intrusive_adapter!(IoFutureAdapter = UnsafeRef<IoFuture>: IoFuture { link: XorLinkedListLink });

impl IoFuture {
    pub const unsafe fn new(op: Entry) -> Self {
        IoFuture {
            state: Inner::Pending(op),
            waker: None,
            link: XorLinkedListLink::new(),
        }
    }
}

impl Future for IoFuture {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = self.get_mut();

        if let Inner::Completed(res) = inner.state {
            return Poll::Ready(res);
        }

        // if we haven't been submitted yet we have to wait for the main loop to do that

        inner.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

pub struct IoRunnable<'a> {
    link: XorLinkedListLink,
    waker: RefCell<Option<Waker>>,
    ring: Cell<*mut IoRing<'a>>,
    future: Pin<&'a mut dyn Future<Output = ()>>,
}

intrusive_adapter!(RunnableAdapter<'a> = &'a IoRunnable<'a>: IoRunnable { link: XorLinkedListLink });

impl<'a> IoRunnable<'a> {
    pub fn new(future: Pin<&'a mut dyn Future<Output = ()>>) -> Self {
        IoRunnable {
            link: XorLinkedListLink::new(),
            waker: RefCell::new(None),
            ring: Cell::new(ptr::null_mut()),
            future,
        }
    }

    unsafe fn from_raw(data: *const ()) -> &'a Self {
        (data as *const Self).as_ref().unwrap()
    }

    fn raw_waker(&self) -> RawWaker {
        unsafe fn waker_clone(data: *const ()) -> RawWaker {
            IoRunnable::from_raw(data).raw_waker()
        }

        unsafe fn waker_wake(data: *const ()) {
            let this = IoRunnable::from_raw(data);
            this.ring
                .get()
                .as_mut()
                .expect("wake called before IoRunnable was polled")
                .runnable
                .push_back(this);
        }

        unsafe fn waker_drop(_data: *const ()) {}

        static WAKER_VTABLE: RawWakerVTable =
            RawWakerVTable::new(waker_clone, waker_wake, waker_wake, waker_drop);

        RawWaker::new(self as *const _ as *const (), &WAKER_VTABLE)
    }

    fn waker(&self) -> Waker {
        unsafe { Waker::from_raw(self.raw_waker()) }
    }
}

pub struct IoRing<'a> {
    ring: IoUring,
    pending: XorLinkedList<IoFutureAdapter>,
    runnable: XorLinkedList<RunnableAdapter<'a>>,
    incomplete: u32,
}

impl<'a> IoRing<'a> {
    pub fn new(ring: IoUring) -> Self {
        IoRing {
            ring,
            pending: XorLinkedList::new(IoFutureAdapter::new()),
            runnable: XorLinkedList::new(RunnableAdapter::new()),
            incomplete: 0,
        }
    }

    pub fn spawn(&mut self, runnable: Pin<&'a mut IoRunnable<'a>>) {
        self.runnable.push_back(runnable.get_mut());
    }

    pub fn submit(&mut self, future: Pin<&mut IoFuture>) {
        let future_ref = future.get_mut();
        let user_data = future_ref as *const _ as u64;
        let state_ref = &mut future_ref.state;
        if let Inner::Pending(op) = mem::replace(state_ref, Inner::Submitted) {
            let op = op.user_data(user_data);
            *state_ref = Inner::Pending(op);
            let future_ref = unsafe { UnsafeRef::from_raw(future_ref) };
            self.pending.push_back(future_ref);
        } else {
            panic!("tried to submit an IoFuture that was not Pending");
        }
    }

    pub fn run(&mut self) -> io::Result<()> {
        loop {
            // Completions make things runnable, and running things leads to submissions, and maybe
            // by the time we're done with all that there'll be some more completions to deal with.
            while self.complete_pending() | self.runnable_pending() | self.submit_pending() {}

            // If everything we've submitted has completed, trying to wait for more completions
            // will block forever, so just return to the caller and let them decide whether to exit
            // or maybe spawn some more I/O.
            if self.incomplete == 0 {
                // Since there are no incomplete submitted operations, nothing could have kept
                // submit_pending from fully draining the pending queue.
                assert!(self.pending.is_empty());

                // Since the loop terminated we know runnable_pending didn't make any progress,
                // which means the runnable queue is empty.
                assert!(self.runnable.is_empty());

                return Ok(());
            }

            // Make sure the kernel has started any submissions we've just made, then block until
            // there's at least one completion in the ring. The manual page for `io_uring_enter(2)`
            // is ambiguous about this, but based on reading the kernel implementation, I don't
            // think they meant it waits for the first new completion after the syscall starts
            // waiting; I believe it should return without blocking if we're racing with an I/O op
            // that completed after the loop above terminated.
            self.ring.submit_and_wait(1)?;
        }
    }

    fn submit_pending(&mut self) -> bool {
        // Don't try to have more submissions in flight at any given time than can fit in the
        // completion queue. On a kernel where ring.params().is_feature_nodrop() is true, the
        // kernel is willing to buffer completions that don't fit in the ring, but relying on that
        // seems like a bad idea to me. Userspace should be responsible for our own buffering. If
        // we hit this limit a lot, the application should allocate bigger rings.
        let cq_entries = self.ring.params().cq_entries();
        if self.pending.is_empty() || self.incomplete >= cq_entries {
            return false;
        }

        let mut made_progress = false;
        let mut queue = self.ring.submission().available();
        while let Some(pending) = self.pending.pop_front() {
            let fut = unsafe { UnsafeRef::into_raw(pending).as_mut().unwrap() };
            if let Inner::Pending(op) = mem::replace(&mut fut.state, Inner::Submitted) {
                if let Err(op) = unsafe { queue.push(op) } {
                    fut.state = Inner::Pending(op);
                    self.pending.push_front(unsafe { UnsafeRef::from_raw(fut) });
                    break;
                }

                made_progress = true;
                self.incomplete += 1;
                if self.incomplete >= cq_entries {
                    break;
                }
            } else {
                unreachable!();
            }
        }
        made_progress
    }

    fn runnable_pending(&mut self) -> bool {
        let mut made_progress = false;
        while let Some(runnable) = self.runnable.pop_front() {
            let old_ring = runnable.ring.replace(self);
            assert!(old_ring.is_null() || old_ring == self);

            let mut waker_ref = runnable.waker.borrow_mut();
            let waker = waker_ref.get_or_insert_with(|| runnable.waker());

            // safety: we never use the mutable reference that's in runnable during the lifetime of
            // this copy of that mutable reference
            let future_ref = unsafe { ptr::read(&runnable.future) };

            // If poll returns Pending, then the future is queued somewhere and not currently
            // runnable. If it returns Ready, then the future has finished and is also not
            // currently runnable. Either way, drop the borrow, but not the memory it points to.
            let _: Poll<()> = future_ref.poll(&mut Context::from_waker(waker));
            made_progress = true;
        }
        made_progress
    }

    fn complete_pending(&mut self) -> bool {
        let mut made_progress = false;
        for completion in self.ring.completion().available() {
            let fut = unsafe { (completion.user_data() as *mut IoFuture).as_mut().unwrap() };
            fut.state = Inner::Completed(completion.result());
            if let Some(waker) = fut.waker.take() {
                waker.wake();
            }

            made_progress = true;
            self.incomplete -= 1;
        }
        made_progress
    }
}
