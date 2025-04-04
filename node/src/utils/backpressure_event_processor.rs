use rsnano_core::utils::BackpressureReceiver;

pub(crate) trait BackpressureEventProcessor<T> {
    fn cool_down(&mut self);
    fn recovered(&mut self);
    fn process(&mut self, event: T);
}

pub(crate) fn spawn_backpressure_processor<T, I>(
    thread_name: impl Into<String>,
    receiver: BackpressureReceiver<I>,
    processor: T,
) where
    I: Send + 'static,
    T: BackpressureEventProcessor<I> + Send + 'static,
{
    std::thread::Builder::new()
        .name(thread_name.into())
        .spawn(move || {
            BackpressureEventLoop::new(receiver, processor).run();
        })
        .unwrap();
}

struct BackpressureEventLoop<T, I>
where
    T: BackpressureEventProcessor<I>,
{
    receiver: BackpressureReceiver<I>,
    processor: T,
    previous_cooldown_state: bool,
}

impl<T, I> BackpressureEventLoop<T, I>
where
    T: BackpressureEventProcessor<I>,
{
    fn new(receiver: BackpressureReceiver<I>, processor: T) -> Self {
        Self {
            receiver,
            processor,
            previous_cooldown_state: false,
        }
    }

    fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            self.process_event(event);
        }
    }

    fn process_event(&mut self, event: I) {
        // Check if we need to cool down the processing to avoid overwhelming the system
        let current_cooldown = self.receiver.should_cool_down();

        if current_cooldown != self.previous_cooldown_state {
            if current_cooldown {
                self.processor.cool_down();
            } else {
                self.processor.recovered();
            }
            self.previous_cooldown_state = current_cooldown;
        }

        self.processor.process(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::utils::backpressure_channel;
    use std::{
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    #[test]
    fn process_events() {
        let (_tx, rx) = backpressure_channel::<&'static str>(2);
        let mut ev_loop = BackpressureEventLoop::new(rx, StubProcessor::default());
        ev_loop.process_event("test1");
        ev_loop.process_event("test2");
        assert_eq!(ev_loop.processor.log(), ["test1", "test2"]);
    }

    #[test]
    fn cool_down_and_recover() {
        let (tx, rx) = backpressure_channel::<&'static str>(2);
        let mut ev_loop = BackpressureEventLoop::new(rx, StubProcessor::default());
        ev_loop.process_event("test1");
        ev_loop.process_event("test2");

        // fill up queue
        tx.send("a").unwrap();
        tx.send("b").unwrap();

        ev_loop.process_event("test3");

        assert_eq!(
            ev_loop.processor.log(),
            ["test1", "test2", "cooldown", "test3"]
        );

        ev_loop.processor.clear();
        ev_loop.process_event("test4");
        ev_loop.receiver.recv().unwrap();
        ev_loop.process_event("test5");

        assert_eq!(ev_loop.processor.log(), ["test4", "recovered", "test5"]);
    }

    #[test]
    fn spawn_processor_thread() {
        let (tx, rx) = backpressure_channel::<&'static str>(2);
        let processor = StubProcessor::default();
        spawn_backpressure_processor("backpres test", rx, processor.clone());
        tx.send("test1").unwrap();
        tx.send("test2").unwrap();
        drop(tx);
        let now = Instant::now();
        while processor.log().len() != 2 && now.elapsed() < Duration::from_secs(5) {
            std::thread::yield_now();
        }
        assert_eq!(processor.log(), ["test1", "test2"])
    }

    #[derive(Clone)]
    struct StubProcessor {
        log: Arc<Mutex<Vec<&'static str>>>,
    }

    impl StubProcessor {
        fn log(&self) -> Vec<&'static str> {
            self.log.lock().unwrap().clone()
        }

        fn clear(&self) {
            self.log.lock().unwrap().clear();
        }
    }

    impl Default for StubProcessor {
        fn default() -> Self {
            Self {
                log: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl BackpressureEventProcessor<&'static str> for StubProcessor {
        fn cool_down(&mut self) {
            self.log.lock().unwrap().push("cooldown");
        }

        fn recovered(&mut self) {
            self.log.lock().unwrap().push("recovered");
        }

        fn process(&mut self, event: &'static str) {
            self.log.lock().unwrap().push(event);
        }
    }
}
