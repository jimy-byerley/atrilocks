use rand::prelude::*;
use std::{
    sync::Arc,
    time::Duration,
    };
use tokio::runtime::Runtime;
use atrilocks::*;

#[test]
fn busy_mutex_simple()   {test_simple::<BusyLock>()}
#[test]
fn busy_mutex_threads()   {test_threads::<BusyLock>()}
#[test]
fn busy_mutex_tasks()   {test_tasks::<BusyLock>()}
#[test]
fn busy_mutex_threads_and_tasks()   {test_threads_and_tasks::<BusyLock>()}

#[test]
fn sleepy_mutex_simple()   {test_simple::<SleepyLock>()}
#[test]
fn sleepy_mutex_threads()   {test_threads::<SleepyLock>()}
#[test]
fn sleepy_mutex_tasks()   {test_tasks::<SleepyLock>()}
#[test]
fn sleepy_mutex_threads_and_tasks()   {test_threads_and_tasks::<SleepyLock>()}


fn test_simple<L: Lock>() {
    let mutex = BusyMutex::<u8>::default();
    
    assert!(mutex.try_lock().is_some());
    assert!(mutex.try_lock().is_some(), "two locking with release in between");
    let guard = mutex.try_lock().unwrap();
    assert!(mutex.try_lock().is_none(), "acquisition while locked");
    drop(guard);
    assert!(mutex.try_lock().is_some(), "acquisition after release");
}

fn test_threads<L: Lock + Default + 'static>() {
    let shared = build_shared::<L>();
    let mut threads = spawn_threads(shared.clone(), 200);
    
    for thread in threads.drain(..) {
        thread.join().unwrap();
    }
    
    final_check(&shared);
}

fn test_tasks<L: Lock + Default + 'static>() {
    let mut runtime = Runtime::new().unwrap();
    
    let shared = build_shared::<L>();
    let mut tasks = spawn_tasks(&mut runtime, shared.clone(), 100);
    
    for task in tasks.drain(..) {
        runtime.block_on(task).unwrap();
    }
    
    final_check(&shared);
}


fn test_threads_and_tasks<L: Lock + Default + 'static>() {
    let mut runtime = Runtime::new().unwrap();
    
    let shared = build_shared::<L>();
    let mut threads = spawn_threads(shared.clone(), 200);
    let mut tasks = spawn_tasks(&mut runtime, shared.clone(), 100);
    
    for thread in threads.drain(..) {
        thread.join().unwrap();
    }
    for task in tasks.drain(..) {
        runtime.block_on(task).unwrap();
    }
    
    final_check(&shared);
}

struct Shared<L: Lock> {
    variants: Vec<[u8; 200]>,
    mutex: Mutex<[u8; 200], L>,
}
fn build_shared<L: Lock + Default>() -> Arc<Shared<L>> {
    const SIZE: usize = 200;
    
    // create many known data variants
    let mut variants = Vec::new();
    for _ in 0 .. 30 {
        let mut variant = [0u8; SIZE];
        for v in variant.iter_mut() {
            *v = rand::rng().random();
        }
        variants.push(variant);
    }
    Arc::new(Shared {
        mutex: Mutex::new(variants[0].clone()),
        variants,
        })
}
fn final_check(shared: &Shared<impl Lock>) {
    let target = shared.mutex.try_lock().expect("deadlock");
    assert!(shared.variants.contains(&*target), "corrupted data: unknown variant found");
}
fn spawn_threads(shared: Arc<Shared<impl Lock + 'static>>, threads: usize) -> Vec<std::thread::JoinHandle<()>> {
    (0 .. threads).map(|_| {
        let shared = shared.clone();
        std::thread::spawn(move || { 
            for _ in 0 .. 100 {
                let duration = Duration::from_micros(rand::rng().random_range(0 .. 100));
                std::thread::sleep(duration);
                
                let mut target = shared.mutex.blocking_lock();
                assert!(shared.variants.contains(&*target), "corrupted data: unknown variant found");
                
                // assumed non atomic copy
                let variant = &shared.variants[rand::rng().random_range(0 .. shared.variants.len())];
                target.copy_from_slice(variant);
            }})
        }).collect::<Vec<_>>()
}
fn spawn_tasks(runtime: &mut Runtime, shared: Arc<Shared<impl Lock + 'static>>, tasks: usize) -> Vec<tokio::task::JoinHandle<()>> {
    (0 .. tasks).map(|_| {
        let shared = shared.clone();
        runtime.spawn(async move { 
            for _ in 0 .. 100 {
                let duration = Duration::from_micros(rand::rng().random_range(0 .. 1000));
                tokio::time::sleep(duration).await;
                
                let mut target = shared.mutex.lock().await;
                assert!(shared.variants.contains(&*target), "corrupted data: unknown variant found");
                
                // assumed non atomic copy
                let variant = &shared.variants[rand::rng().random_range(0 .. shared.variants.len())];
                target.copy_from_slice(variant);
            }})
        }).collect::<Vec<_>>()
}
