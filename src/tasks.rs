use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::{
    atomic::{AtomicPtr, AtomicUsize, Ordering},
    Arc, Mutex, Weak,
};
use std::time::Instant;
use std::{
    fmt::{self, Write},
    ptr,
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

pub struct TaskData {
    pub future: String,
    pub scope: String,
    pub kind: String,
    pub created: Instant,
    _p: (),
}

#[derive(Clone)]
pub struct TaskList(Arc<[Mutex<Vec<Weak<TaskData>>>]>);

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
                    Ordering::AcqRel,
                );
            } else {
                self.blocking_meta.compare_and_swap(
                    ptr::null_mut(),
                    meta as *const _ as *mut _,
                    Ordering::AcqRel,
                );
            }
            subscriber::Interest::always()
        } else {
            subscriber::Interest::never()
        }
    }

    fn new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, cx: Context<'_, S>) {
        let meta = attrs.metadata();
        if ptr::eq(
            self.task_meta.load(Ordering::Acquire),
            meta as *const _ as *mut _,
        ) || ptr::eq(
            self.blocking_meta.load(Ordering::Acquire),
            meta as *const _ as *mut _,
        ) {
            let created = Instant::now();
            let mut task_data = TaskData {
                future: String::new(),
                scope: String::new(),
                kind: String::new(),
                created,
                _p: (),
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
}

impl field::Visit for TaskData {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        match field.name() {
            "future" => write!(&mut self.future, "{:?}", value).unwrap(),
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
                let id = NEXT.fetch_add(1, Ordering::Relaxed) % self.0.len();
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
