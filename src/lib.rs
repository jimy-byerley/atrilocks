/*!
    collection of light weight async synchronization primitives based on atomics
    
    the name **atrilocks** stands for *Asynchronous Atomic-based runtime Agnostic LOCKS*
*/
use core::{
    sync::atomic::{AtomicBool, Ordering::*},
    future::{Future, poll_fn},
    task::{Poll, Context, Waker},
    cell::UnsafeCell,
    pin::Pin,
    ops::{Deref, DerefMut},
    };


/// object that calls the given function when dropped
pub struct OnDrop<F: FnOnce() -> ()> {
    callback: Option<F>,
}
impl<F: FnOnce() -> ()> OnDrop<F> {
    pub fn new(callback: F) -> Self {
        Self{callback: Some(callback)}
    }
    pub fn cancel(&mut self) -> Option<F> {
        self.callback.take()
    }
}
impl<F> Drop for OnDrop<F>
where F: FnOnce() -> () {
    fn drop(&mut self)   {
        self.callback.take().map(|f| f());
    }
}

/** 
    heapless synchronization point for unlimited amount of futures, with sleepy wait of notifications
    
    implemented using an atomic and a waker
*/
pub struct Notify {
    waker: BusyMutex<Option<Waker>>,
}
impl Notify {
    /**
        block until trigger is called and all waiturs conditions until the current task are satisfied
        
        if you want all pending tasks to be awaken, make sure all tasks put the same condition
        
        the wait is not busy but sleeping wait, and depends on the runtime to wake tasks
        
        this task is cancelable without influence on other awaiting tasks
    */
    pub fn wait<F, R>(&self, condition: F) -> Waiter<'_, F>
    where F: FnMut() -> Option<R>
    {
        Waiter {
            condition,
            notify: self,
            next: None,
            }
    }
    /// awake current pending tasks in random order until conditions are not met
    pub fn trigger(&self) {
        // wake the first pending task, they will all wake in a chain
        if let Some(waker) = self.waker.blocking_lock().take() {
            waker.wake();
        }
    }
}
pub struct Waiter<'n, F> {
    notify: &'n Notify,
    condition: F,
    next: Option<Waker>,
}
impl<R, F> Future for Waiter<'_, F>
where F: FnMut() -> Option<R>
{
    type Output = R;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<R> {
        if let Some(result) = (self.as_mut().condition)() {
            // wake next one only now that condition is met here
            if let Some(next) = self.as_mut().next.take() {
                next.wake();
            }
            Poll::Ready(result)
        }
        else {
            self.next = self.notify.waker.blocking_lock().replace(context.waker().clone());
            Poll::Pending
        }
    }
}
impl<F> Drop for Waiter<'_, F> {
    fn drop(&mut self) {
        // as this future disapears, eventual next would not be awaken, wake it so it can check its condition and register again if not met
        if let Some(next) = self.next.take() {
            next.wake();
        }
    }
}
impl<F> Unpin for Waiter<'_, F> {}


/// trait for async lock implementations
pub trait Lock {
    /// return true if in *locked* state at the moment it is called
    fn is_locked(&self) -> bool;
    /// try to acquire the lock, then return true, otherwise return false, on success it will remain in *locked* state until [Self::release] is called
    fn try_lock(&self) -> bool;
    /// block until the lock is acquired (asynchronous version), it will remain in *locked* state until [Self::release] is called
    fn lock(&self) -> impl Future;
    /// block until the lock is acquired (synchronous version), it will remain in *locked* state until [Self::release] is called
    fn blocking_lock(&self);
    /// set the lock to *unlocked* state, whatever its previous state was
    fn release(&self);
}

/** 
    extremely light weight lock primitive whose locking calls are busy waiting (constantly polling for acquisition)
    
    implemented using only a atomic bool
    
    the difference between this primitive and a mutex is that this one doesn't protect any data, but on the other hand allows that lock and release are not tied to scopes and thus allows for putting locking tasks and unlocking them one by one others
*/
pub struct BusyLock {
    locked: AtomicBool,
}
impl Lock for BusyLock {
    fn is_locked(&self) -> bool {
        self.locked.load(Relaxed)
    }
    fn try_lock(&self) -> bool {
        self.locked.swap(true, Acquire) == false
    }
    fn lock(&self) -> impl Future {
        poll_fn(|_| match self.try_lock() {
            true => Poll::Ready(()),
            false => Poll::Pending,
        })
    }
    fn blocking_lock(&self) {
        while ! self.try_lock() {}
    }
    fn release(&self) {
        self.locked.store(false, Release);
    }
}

/**
    runtime agnotstic heapless lock primitive whose calls are using using the async runtime context to wake acquiring tasks only on release
    
    implemented using 2 atomics and a waker
    
    [Self::blocking_lock] is still busy waiting, but yielding to the kernel if `std` feature is enabled
*/
pub struct SleepyLock {
    lock: BusyLock,
    notify: Notify,
}
impl Lock for SleepyLock {
    fn is_locked(&self) -> bool {
        self.lock.is_locked()
    }
    fn try_lock(&self) -> bool {
        self.lock.try_lock()
    }
    fn lock(&self) -> impl Future {
        self.notify.wait(|| if self.try_lock() {Some(())} else {None})
    }
    fn blocking_lock(&self) {
        while ! self.try_lock() {
            #[cfg(feature = "std")]
            std::thread::yield_now();
        }
    }
    fn release(&self) {
        self.lock.release();
        self.notify.trigger();
    }
}


/// mutex based on busy waiting while the mutex is locked by something else
pub type BusyMutex<T> = Mutex<T, BusyLock>;
/// mutex setting tasks asleep while the mutex is locked by something else
pub type SleepyMutex<T> = Mutex<T, SleepyLock>;

/** 
    extremely light wieght mutex whose locking calls are busy waiting (constantly polling for acquisition)
    
    implemented using only a atomic bool
*/
pub struct Mutex<T, L: Lock> {
    value: UnsafeCell<T>,
    lock: L,
}
impl<T, L: Lock> Mutex<T, L> {
    /// return true if mutex is already locked
    pub fn is_locked(&self) -> bool {
        self.lock.is_locked()
    }
    /// try to acquire the mutex, making only one attempt, never blocking
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T,L>> {
        if self.lock.try_lock() {
            Some(MutexGuard{mutex: self})
        }
        else {None}
    }
    /// wait until the mutex is unlocked then acquire it, might busy wait depending on the underlying lock. 
    pub async fn lock(&self) -> MutexGuard<'_, T,L> {
        self.lock.lock().await;
        MutexGuard{mutex: self}
    }
    /// wait until the mutex is unlocked then acquire it, might busy wait depending on the underlying lock
    pub fn blocking_lock(&self) -> MutexGuard<'_, T,L> {
        self.lock.blocking_lock();
        MutexGuard{mutex: self}
    }
}

pub struct MutexGuard<'m, T, L: Lock> {
    mutex: &'m Mutex<T, L>,
}
impl<T, L: Lock> Deref for MutexGuard<'_, T, L> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe {& *self.mutex.value.get()}
    }
}
impl<T, L: Lock> DerefMut for MutexGuard<'_, T, L> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe {&mut *self.mutex.value.get()}
    }
}
impl<T, L: Lock> Drop for MutexGuard<'_, T, L> {
    fn drop(&mut self) {
        self.mutex.lock.release();
    }
}
