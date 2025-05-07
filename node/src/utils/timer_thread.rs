use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use rsnano_core::utils::{CancellationToken, Runnable};
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};

// Runs a task periodically in it's own thread
pub struct TimerThread<T: Runnable + 'static> {
    thread_name: String,
    task: Mutex<Option<T>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    cancel_token: CancellationToken,
    start_listener: OutputListenerMt<TimerStartEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerStartEvent {
    pub thread_name: String,
    pub start_type: TimerStartType,
    pub interval: Duration,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TimerStartType {
    Start,
    StartDelayed,
    RunOnceThenStart,
}

impl<T: Runnable> TimerThread<T> {
    pub fn new(name: impl Into<String>, task: T) -> Self {
        Self {
            thread_name: name.into(),
            task: Mutex::new(Some(task)),
            thread: Mutex::new(None),
            cancel_token: CancellationToken::new(),
            start_listener: OutputListenerMt::new(),
        }
    }

    pub fn task(&self) -> std::sync::MutexGuard<Option<T>> {
        self.task.lock().unwrap()
    }

    pub fn is_running(&self) -> bool {
        self.thread.lock().unwrap().is_some()
    }

    pub fn track_start(&self) -> Arc<OutputTrackerMt<TimerStartEvent>> {
        self.start_listener.track()
    }

    /// Start the thread which periodically runs the task
    pub fn start(&self, interval: Duration) {
        self.start_impl(interval, TimerStartType::Start);
    }

    /// Starts the thread and waits for the given interval before the first run
    pub fn start_delayed(&self, interval: Duration) {
        self.start_impl(interval, TimerStartType::StartDelayed);
    }

    /// Runs the task in the current thread once before the thread is started
    pub fn run_once_then_start(&self, interval: Duration) {
        self.start_impl(interval, TimerStartType::RunOnceThenStart);
    }

    fn start_impl(&self, interval: Duration, start_type: TimerStartType) {
        self.start_listener.emit(TimerStartEvent {
            thread_name: self.thread_name.clone(),
            interval,
            start_type,
        });

        let mut task = self
            .task
            .lock()
            .unwrap()
            .take()
            .expect("task already taken");

        let cancel_token = self.cancel_token.clone();

        if start_type == TimerStartType::RunOnceThenStart {
            task.run(&cancel_token);
        }

        let mut timer_loop = TimerLoop {
            interval,
            start_type,
            cancel_token,
            task,
            next_wait_duration: interval,
        };

        let handle = std::thread::Builder::new()
            .name(self.thread_name.clone())
            .spawn(move || timer_loop.run())
            .unwrap();

        *self.thread.lock().unwrap() = Some(handle);
    }

    pub fn stop(&self) {
        self.cancel_token.cancel();
        let handle = self.thread.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap();
        }
    }
}

struct TimerLoop<T: Runnable> {
    interval: Duration,
    start_type: TimerStartType,
    cancel_token: CancellationToken,
    task: T,
    next_wait_duration: Duration,
}

impl<T: Runnable> TimerLoop<T> {
    fn run(&mut self) {
        if self.start_type == TimerStartType::Start {
            self.run_one();
        }

        loop {
            if self
                .cancel_token
                .wait_for_cancellation(self.next_wait_duration)
            {
                break;
            }
            self.run_one();
        }
    }

    fn run_one(&mut self) {
        let start = Instant::now();
        self.task.run(&self.cancel_token);
        let elapsed = start.elapsed();
        self.next_wait_duration = self.interval.saturating_sub(elapsed);
    }
}

impl<T: Runnable> Drop for TimerThread<T> {
    fn drop(&mut self) {
        self.stop();
    }
}
