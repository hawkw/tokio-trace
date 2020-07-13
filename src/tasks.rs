use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::{mpsc, Arc, AtomicPtr, Ordering, Weak};
use std::{fmt, ptr};
use tracing_core::{
    field::{self, Field},
    span,
    subscriber::{self, Interest},
    Metadata,
};
use tracing_subscriber::{
    fmt::{
        format::{DefaultFields, FormatFields},
        FormattedFields,
    },
    layer::Context,
    Layer, LookupSpan,
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
    pub created: quanta::Instant,
    _p: (),
}

#[derive(Clone)]
pub struct TaskList(Arc<[Mutex<Vec<Weak<TaskData>>>]>);

impl<F> TaskLayer<F> {
    pub fn new() -> (TaskList, Self) {
        let list = TaskList::new();
        let layer = Self {
            list: list.clone(),
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
            if meta.fields().any(|f| f.name == "future") {
                self.task_meta
                    .compare_and_swap(ptr::null_mut(), meta as *mut _, Ordering::AcqRel);
            } else {
                self.blocking_meta.compare_and_swap(
                    ptr::null_mut(),
                    meta as *mut _,
                    Ordering::AcqRel,
                );
            }
        }
    }

    fn new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, cx: Context<'_, S>) {
        let meta = attrs.metadata();
        if ptr::eq(self.task_meta.load(Ordering::Acquire), meta as *mut _)
            || ptr::eq(self.blocking_meta.load(Ordering::Acquire), meta as *mut _)
        {
            let created = quanta::Instant::now();
            let mut task_data = TaskData {
                future: String::new(),
                scope: String::new(),
                kind: String::new(),
                created,
                _p: (),
            };
            let span = cx.span(id).expect("span must exist");
            for span in span.scope() {
                write!(&mut task_data.scope, "\t{}", span.name());
                let exts = span.extensions();
                if let Some(fields) = exts.get::<FormattedFields<F>>() {
                    write!(&mut task_data.scope, "{{{}}}", fields.fields);
                }
            }
            attrs.record(&mut task_data);
            let task_data = Arc::new(task_data);
            let weak = Arc::downgrade(&task_data);
            span.extensions_mut().insert(task_data);
            self.tx.send(weak);
        }
    }
}

impl field::Visit for TaskData {
    fn record_debug(&mut self, field: &Field, value: &mut dyn fmt::Debug) {
        match field.name() {
            "future" => write!(&mut self.future, "{:?}", value),
            "kind" => write!(&mut self.kind, "{:?}", value),
            _ => {}
        }
    }
}

impl TaskList {
    fn new() -> Self {
        Self(Arc::from(vec![Mutex::new(Vec::new()); num_cpus::get()]))
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

    fn tasks(&self, mut f: impl FnMut(&TaskData)) {
        for shard in self.0 {
            let shard = shard.lock().unwrap();
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
