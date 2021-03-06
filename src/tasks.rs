use std::sync::{
    atomic::{AtomicPtr, AtomicUsize, Ordering::*},
    Arc, Mutex, MutexGuard, Weak,
};
use std::{
    cell::Cell,
    fmt::{self, Write},
    marker::PhantomData,
    ptr,
    time::{Duration, Instant},
};
use tracing_core::{
    field::{self, Field},
    span,
    subscriber::{self, Subscriber},
    Metadata,
};
use tracing_subscriber::{
    fmt::{
        format::{DefaultFields, FormatFields},
        FormattedFields,
    },
    layer::Context,
    registry::LookupSpan,
    Layer,
};

pub struct TasksLayer<F = DefaultFields> {
    tasks: TaskList,
    task_meta: AtomicPtr<Metadata<'static>>,
    blocking_meta: AtomicPtr<Metadata<'static>>,
    _f: PhantomData<fn(F)>,
}

#[derive(Debug)]
pub struct TaskData {
    pub location: String,
    pub scope: String,
    pub kind: String,
    polls: AtomicUsize,
    currently_in: AtomicUsize,
    timings: Mutex<TimeData>,
}

#[derive(Debug)]
pub struct Timings<'a> {
    data: MutexGuard<'a, TimeData>,
    now: Instant,
}

#[derive(Debug)]
struct TimeData {
    created: Instant,
    first_poll: Option<Instant>,
    last_entered: Option<Instant>,
    busy_time: Duration,
}

#[derive(Clone)]
pub struct TaskList(Arc<[Mutex<Vec<Weak<TaskData>>>]>);

pub fn tasks() -> (TaskList, TasksLayer) {
    TasksLayer::new()
}

impl<F> TasksLayer<F> {
    pub fn new() -> (TaskList, Self) {
        let list = TaskList::new();
        let layer = Self {
            tasks: list.clone(),
            task_meta: AtomicPtr::new(ptr::null_mut()),
            blocking_meta: AtomicPtr::new(ptr::null_mut()),
            _f: PhantomData,
        };
        (list, layer)
    }
}

impl<F> TasksLayer<F> {
    #[inline(always)]
    fn cares_about(&self, meta: &'static Metadata<'static>) -> bool {
        ptr::eq(self.task_meta.load(Relaxed), meta as *const _ as *mut _)
            || ptr::eq(self.blocking_meta.load(Relaxed), meta as *const _ as *mut _)
    }
}

impl<S, F> Layer<S> for TasksLayer<F>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
    F: for<'writer> FormatFields<'writer> + 'static,
{
    fn register_callsite(&self, meta: &'static Metadata<'static>) -> subscriber::Interest {
        if meta.target() == "tokio::task" && meta.name() == "task" {
            if meta.fields().iter().any(|f| f.name() == "future") {
                self.task_meta.compare_and_swap(
                    ptr::null_mut(),
                    meta as *const _ as *mut _,
                    AcqRel,
                );
            } else {
                self.blocking_meta.compare_and_swap(
                    ptr::null_mut(),
                    meta as *const _ as *mut _,
                    AcqRel,
                );
            }
        }

        subscriber::Interest::always()
    }

    fn new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, cx: Context<'_, S>) {
        let meta = attrs.metadata();
        if self.cares_about(meta) {
            let created = Instant::now();
            let mut task_data = TaskData {
                location: String::new(),
                scope: String::new(),
                kind: String::new(),
                currently_in: AtomicUsize::new(0),
                polls: AtomicUsize::new(0),
                timings: Mutex::new(TimeData {
                    created,
                    first_poll: None,
                    last_entered: None,
                    busy_time: Duration::from_secs(0),
                }),
            };
            let span = cx.span(id).expect("span must exist");
            for span in span.parents() {
                write!(&mut task_data.scope, "\t{}", span.name()).unwrap();
                let exts = span.extensions();
                if let Some(fields) = exts.get::<FormattedFields<F>>() {
                    write!(&mut task_data.scope, "{{{}}}", fields.fields).unwrap();
                }
            }
            attrs.record(&mut task_data);
            let task_data = Arc::new(task_data);
            let weak = Arc::downgrade(&task_data);
            span.extensions_mut().insert(task_data);
            self.tasks.insert(weak);
        }
    }

    fn on_enter(&self, id: &span::Id, cx: Context<'_, S>) {
        if let Some(span) = cx.span(id) {
            if !self.cares_about(span.metadata()) {
                return;
            }
            let now = Instant::now();
            let exts = span.extensions();
            if let Some(task) = exts.get::<Arc<TaskData>>() {
                let currently_in = task.currently_in.fetch_add(1, AcqRel);
                task.polls.fetch_add(1, Release);
                // If we are the first thread to enter this span, update the
                // timestamps.
                if currently_in == 0 {
                    // Safe to lock!
                    let mut timings = task.timings.lock().unwrap();
                    if timings.first_poll.is_none() {
                        timings.first_poll = Some(now)
                    }
                    debug_assert!(timings.last_entered.is_none());
                    timings.last_entered = Some(now);
                }
            }
        }
    }

    fn on_exit(&self, id: &span::Id, cx: Context<'_, S>) {
        if let Some(span) = cx.span(id) {
            if !self.cares_about(span.metadata()) {
                return;
            }
            let now = Instant::now();
            let exts = span.extensions();
            if let Some(task) = exts.get::<Arc<TaskData>>() {
                let currently_in = task.currently_in.fetch_sub(1, AcqRel);

                // If we are the last thread to enter this span, update the
                // timestamps.
                if currently_in == 1 {
                    // Safe to lock!
                    let mut timings = task.timings.lock().unwrap();
                    if timings.first_poll.is_none() {
                        timings.first_poll = Some(now)
                    }
                    let last_entered = timings
                        .last_entered
                        .take()
                        .expect("task must be entered to be exited");
                    let delta = now.duration_since(last_entered);
                    timings.busy_time += delta;
                }
            }
        }
    }
}

impl field::Visit for TaskData {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        match field.name() {
            "spawn.location" => write!(&mut self.location, "{:?}", value).unwrap(),
            "kind" => write!(&mut self.kind, "{:?}", value).unwrap(),
            _ => {}
        }
    }
}

impl TaskList {
    fn new() -> Self {
        Self(
            (0..num_cpus::get())
                .map(|_| Mutex::new(Vec::new()))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    fn insert(&self, task: Weak<TaskData>) {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        // XXX(eliza): if this was indexed by _current_ CPU core, instead,
        // inserts would *never* contend.
        std::thread_local! {
            static ID: Cell<Option<usize>> = Cell::new(None);
        }
        let idx = ID.with(|curr| {
            if let Some(id) = curr.get() {
                id
            } else {
                let id = NEXT.fetch_add(1, Relaxed) % self.0.len();
                curr.set(Some(id));
                id
            }
        });
        self.0[idx].lock().unwrap().push(task);
    }

    pub fn tasks(&self, mut f: impl FnMut(&TaskData)) {
        for shard in self.0.iter() {
            let mut shard = shard.lock().unwrap();
            shard.retain(|weak| {
                if let Some(task) = weak.upgrade() {
                    f(&task);
                    true
                } else {
                    false
                }
            })
        }
    }
}

impl TaskData {
    pub fn timings(&self) -> Timings<'_> {
        Timings {
            data: self.timings.lock().unwrap(),
            now: Instant::now(),
        }
    }

    pub fn is_active(&self) -> bool {
        self.currently_in.load(Acquire) > 0
    }

    pub fn polls(&self) -> usize {
        self.polls.load(Acquire)
    }
}

impl<'a> Timings<'a> {
    pub fn to_first_poll(&self) -> Option<Duration> {
        Some(self.data.first_poll?.duration_since(self.data.created))
    }

    pub fn busy_time(&self) -> Duration {
        if let Some(last_entered) = self.data.last_entered {
            return self.data.busy_time + self.now.duration_since(last_entered);
        }

        self.data.busy_time
    }

    pub fn total_time(&self) -> Duration {
        self.now.duration_since(self.data.created)
    }

    pub fn idle_time(&self) -> Duration {
        self.total_time() - self.busy_time()
    }
}

#[cfg(feature = "serde")]
mod serde {
    use super::{TaskData, TaskList, Timings};
    use serde::ser::{Serialize, SerializeSeq, SerializeStruct, Serializer};

    impl Serialize for TaskData {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut state = serializer.serialize_struct("TaskData", 6)?;
            state.serialize_field("kind", &self.kind)?;
            state.serialize_field("is_active", &self.is_active())?;
            state.serialize_field("polls", &self.polls())?;
            state.serialize_field("scope", &self.scope)?;
            state.serialize_field("location", &self.location)?;
            state.serialize_field("timings", &self.timings())?;
            state.end()
        }
    }

    impl Serialize for Timings<'_> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut state = serializer.serialize_struct("Timings", 4)?;
            state.serialize_field(
                "to_first_poll_ns",
                &self.to_first_poll().map(|t| t.as_nanos()),
            )?;
            state.serialize_field("busy_ns", &self.busy_time().as_nanos())?;
            state.serialize_field("idle_ns", &self.idle_time().as_nanos())?;
            state.serialize_field("total_ns", &self.total_time().as_nanos())?;
            state.end()
        }
    }

    impl Serialize for TaskList {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut state = serializer.serialize_seq(None)?;
            let mut res = Ok(());
            self.tasks(|task| {
                if res.is_ok() {
                    res = state.serialize_element(task).map(|_| ());
                }
            });
            res?;
            state.end()
        }
    }
}
