use chrono::{DateTime, Utc};
use std::{
    collections::HashMap,
    num::NonZeroU64,
    ops::DerefMut,
    sync::{Mutex, MutexGuard},
};
use tracing::{
    Level, Subscriber,
    field::{Field, Visit},
    span,
};
use tracing_subscriber::{
    EnvFilter, Layer, Registry, layer::SubscriberExt, registry::LookupSpan, util::SubscriberInitExt,
};

pub fn install<T: TapeMachine>(machine: T) {
    let filter = std::env::var("RUST_LOG").unwrap_or("warn".to_string());

    match Registry::default()
        .with(EnvFilter::from(&filter))
        .with(TapeMachineLogger::new(machine))
        .try_init()
    {
        Ok(()) => tracing::debug!(?filter, "Logger initialized"),
        Err(e) => {
            tracing::warn!(%e, "Trying to initialize logger twice");
            tracing::debug!(?e);
        }
    }
}

pub trait TapeMachine: Send + 'static {
    fn needs_restart(&mut self) -> bool;
    fn handle(&mut self, instruction: Instruction);
}

#[derive(Clone, Copy)]
pub enum Instruction<'a> {
    Restart,
    NewString(&'a str),
    NewSpan {
        parent: Option<NonZeroU64>,
        span: NonZeroU64,
        name: CacheString<'a>,
    },
    FinishedSpan,
    NewRecord(NonZeroU64),
    FinishedRecord,
    StartEvent {
        time: DateTime<Utc>,
        span: Option<NonZeroU64>,
        target: CacheString<'a>,
        priority: Level,
    },
    FinishedEvent,
    AddValue(FieldValue<'a>),
    DeleteSpan(NonZeroU64),
}
impl Instruction<'_> {
    pub fn id(self) -> InstructionId {
        match self {
            Instruction::Restart => InstructionId::Restart,
            Instruction::NewString(..) => InstructionId::NewString,
            Instruction::NewSpan { .. } => InstructionId::NewSpan,
            Instruction::FinishedSpan => InstructionId::FinishedSpan,
            Instruction::NewRecord(..) => InstructionId::NewRecord,
            Instruction::FinishedRecord => InstructionId::FinishedRecord,
            Instruction::StartEvent { .. } => InstructionId::StartEvent,
            Instruction::FinishedEvent => InstructionId::FinishedEvent,
            Instruction::AddValue(..) => InstructionId::AddValue,
            Instruction::DeleteSpan(..) => InstructionId::DeleteSpan,
        }
    }
}

#[derive(Clone, Copy)]
pub enum InstructionId {
    Restart,
    NewString,
    NewSpan,
    FinishedSpan,
    NewRecord,
    FinishedRecord,
    StartEvent,
    FinishedEvent,
    AddValue,
    DeleteSpan,
}
impl From<InstructionId> for u8 {
    fn from(val: InstructionId) -> Self {
        match val {
            InstructionId::Restart => 255,
            InstructionId::NewString => 1,
            InstructionId::NewSpan => 2,
            InstructionId::FinishedSpan => 4,
            InstructionId::NewRecord => 8,
            InstructionId::FinishedRecord => 16,
            InstructionId::StartEvent => 32,
            InstructionId::FinishedEvent => 64,
            InstructionId::AddValue => 128,
            InstructionId::DeleteSpan => 0,
        }
    }
}
impl TryFrom<u8> for InstructionId {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            255 => InstructionId::Restart,
            1 => InstructionId::NewString,
            2 => InstructionId::NewSpan,
            4 => InstructionId::FinishedSpan,
            8 => InstructionId::NewRecord,
            16 => InstructionId::FinishedRecord,
            32 => InstructionId::StartEvent,
            64 => InstructionId::FinishedEvent,
            128 => InstructionId::AddValue,
            0 => InstructionId::DeleteSpan,
            e => return Err(e),
        })
    }
}

#[derive(Clone, Copy)]
pub struct FieldValue<'a> {
    pub name: CacheString<'a>,
    pub value: Value<'a>,
}
impl FieldValue<'_> {
    pub fn to_owned(self) -> FieldValueOwned {
        FieldValueOwned {
            name: self.name.to_owned(),
            value: self.value.to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct FieldValueOwned {
    pub name: CacheStringOwned,
    pub value: ValueOwned,
}

#[derive(Clone, Copy)]
pub enum Value<'a> {
    String(CacheString<'a>),
    Float(f64),
    Integer(i64),
    Unsigned(u64),
    Bool(bool),
    ByteArray(&'a [u8]),
}
impl Value<'_> {
    pub fn to_owned(self) -> ValueOwned {
        match self {
            Value::String(cache_string) => ValueOwned::String(cache_string.to_owned()),
            Value::Float(value) => ValueOwned::Float(value),
            Value::Integer(value) => ValueOwned::Integer(value),
            Value::Unsigned(value) => ValueOwned::Unsigned(value),
            Value::Bool(value) => ValueOwned::Bool(value),
            Value::ByteArray(value) => ValueOwned::ByteArray(value.to_owned()),
        }
    }
}
impl<'a> From<CacheString<'a>> for Value<'a> {
    fn from(value: CacheString<'a>) -> Self {
        Self::String(value)
    }
}
impl From<f64> for Value<'_> {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}
impl From<i64> for Value<'_> {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}
impl From<u64> for Value<'_> {
    fn from(value: u64) -> Self {
        Self::Unsigned(value)
    }
}
impl From<bool> for Value<'_> {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}
impl<'a> From<&'a [u8]> for Value<'a> {
    fn from(value: &'a [u8]) -> Self {
        Value::ByteArray(value)
    }
}

#[derive(Clone)]
pub enum ValueOwned {
    String(CacheStringOwned),
    Float(f64),
    Integer(i64),
    Unsigned(u64),
    Bool(bool),
    ByteArray(Vec<u8>),
}

#[derive(Clone, Copy)]
pub enum CacheString<'a> {
    Small(&'a str),
    Cached(u64),
}
impl CacheString<'_> {
    pub fn to_owned(self) -> CacheStringOwned {
        match self {
            CacheString::Small(value) => CacheStringOwned::Small(value.to_owned()),
            CacheString::Cached(value) => CacheStringOwned::Cached(value),
        }
    }
}

#[derive(Clone)]
pub enum CacheStringOwned {
    Small(String),
    Cached(u64),
}
impl CacheStringOwned {
    pub fn read<'a>(&'a self, strings: &'a [String]) -> &'a str {
        match self {
            CacheStringOwned::Small(str) => str,
            CacheStringOwned::Cached(index) => strings[*index as usize].as_str(),
        }
    }
}

pub struct TapeMachineLogger<T> {
    inner: Mutex<TapeMachineLoggerInner<T>>,
}
impl<T> TapeMachineLogger<T>
where
    T: TapeMachine,
{
    pub fn new(mut machine: T) -> Self {
        machine.handle(Instruction::Restart);
        TapeMachineLogger {
            inner: Mutex::new(TapeMachineLoggerInner {
                machine,
                strings: Default::default(),
                recover: Default::default(),
            }),
        }
    }

    fn machine(&self) -> MutexGuard<'_, TapeMachineLoggerInner<T>> {
        let mut machine = self.inner.lock().unwrap();
        if machine.machine.needs_restart() {
            machine.restart();
        }
        machine
    }
}
impl<T, S> Layer<S> for TapeMachineLogger<T>
where
    T: TapeMachine,
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut machine = self.machine();
        let name = machine.cache_string(attrs.metadata().name());
        let span = ctx.span(id).unwrap();
        machine.handle(Instruction::NewSpan {
            parent: span.parent().map(|parent| parent.id().into_non_zero_u64()),
            span: id.into_non_zero_u64(),
            name,
        });
        attrs.record(&mut VisitMachine(machine.deref_mut()));
        machine.handle(Instruction::FinishedSpan);
    }

    fn on_record(
        &self,
        id: &span::Id,
        values: &span::Record<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut machine = self.machine();
        machine.handle(Instruction::NewRecord(id.into_non_zero_u64()));
        values.record(&mut VisitMachine(machine.deref_mut()));
        machine.handle(Instruction::FinishedRecord);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut machine = self.machine();

        let time = Utc::now();
        let span = ctx
            .event_span(event)
            .map(|span| span.id().into_non_zero_u64());
        let priority = *event.metadata().level();
        let target = machine.cache_string(event.metadata().target());
        machine.handle(Instruction::StartEvent {
            time,
            span,
            target,
            priority,
        });
        event.record(&mut VisitMachine(machine.deref_mut()));

        machine.handle(Instruction::FinishedEvent);
    }

    fn on_close(&self, id: span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut machine = self.machine();
        machine.handle(Instruction::DeleteSpan(id.into_non_zero_u64()));
    }
}

struct TapeMachineLoggerInner<T> {
    machine: T,
    strings: HashMap<String, u64>,
    recover: RecoverSpan,
}
impl<T> TapeMachineLoggerInner<T>
where
    T: TapeMachine,
{
    fn cache_string<'a>(&mut self, string: &'a str) -> CacheString<'a> {
        Self::do_cache_string(&mut self.machine, &mut self.strings, string)
    }

    fn do_cache_string<'a>(
        machine: &mut T,
        strings: &mut HashMap<String, u64>,
        string: &'a str,
    ) -> CacheString<'a> {
        if let Some(id) = strings.get(string) {
            return CacheString::Cached(*id);
        }

        let id = strings.len() as u64;
        let small = !matches!(
            (id, string.len()),
            (0..=0xffff, 4..)
                | (0x1_0000..=0xff_ffff, 5..)
                | (0x100_0000..=0xff_ffff_ffff, 7..)
                | (_, 11..)
        );

        if small {
            CacheString::Small(string)
        } else {
            machine.handle(Instruction::NewString(string));
            strings.insert(string.to_owned(), id);
            CacheString::Cached(id)
        }
    }

    fn field_value<'a, V>(&mut self, field: &Field, value: V) -> FieldValue<'a>
    where
        V: Into<Value<'a>>,
    {
        let name = self.cache_string(field.name());
        let value = value.into();

        FieldValue { name, value }
    }

    fn handle(&mut self, instruction: Instruction) {
        self.recover.handle(instruction);
        self.machine.handle(instruction);
    }

    fn restart(&mut self) {
        let mut strings = vec![String::default(); self.strings.len()];
        for (string, index) in std::mem::take(&mut self.strings).into_iter() {
            strings[index as usize] = string;
        }

        self.machine.handle(Instruction::Restart);

        for (span, records) in self.recover.span.iter() {
            let name = records.name.read(&strings);
            let name = Self::do_cache_string(&mut self.machine, &mut self.strings, name);
            self.machine.handle(Instruction::NewSpan {
                parent: records.parent,
                span: *span,
                name,
            });

            for record in records.records.iter() {
                let name = record.name.read(&strings);
                let name = Self::do_cache_string(&mut self.machine, &mut self.strings, name);
                let value: Value = match &record.value {
                    ValueOwned::String(str) => {
                        let str = str.read(&strings);
                        Self::do_cache_string(&mut self.machine, &mut self.strings, str).into()
                    }
                    ValueOwned::Float(value) => (*value).into(),
                    ValueOwned::Integer(value) => (*value).into(),
                    ValueOwned::Unsigned(value) => (*value).into(),
                    ValueOwned::Bool(value) => (*value).into(),
                    ValueOwned::ByteArray(items) => Value::ByteArray(items),
                };

                self.machine
                    .handle(Instruction::AddValue(FieldValue { name, value }));
            }

            self.machine.handle(Instruction::FinishedSpan);
        }
    }
}

struct VisitMachine<'a, T>(&'a mut TapeMachineLoggerInner<T>);
impl<T> Visit for VisitMachine<'_, T>
where
    T: TapeMachine,
{
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record_str(field, &format!("{value:?}"));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        let value = self.0.field_value(field, value);
        self.0.handle(Instruction::AddValue(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        let value = self.0.field_value(field, value);
        self.0.handle(Instruction::AddValue(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        let value = self.0.field_value(field, value);
        self.0.handle(Instruction::AddValue(value));
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.record_bytes(field, &value.to_le_bytes());
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.record_bytes(field, &value.to_le_bytes());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        let value = self.0.field_value(field, value);
        self.0.handle(Instruction::AddValue(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        let value = self.0.cache_string(value);
        let value = self.0.field_value(field, value);
        self.0.handle(Instruction::AddValue(value));
    }

    fn record_bytes(&mut self, field: &Field, value: &[u8]) {
        let value = self.0.field_value(field, value);
        self.0.handle(Instruction::AddValue(value));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.record_str(field, &value.to_string())
    }
}

pub struct SpanRecords {
    pub parent: Option<NonZeroU64>,
    pub name: CacheStringOwned,
    pub records: Vec<FieldValueOwned>,
}

#[derive(Default)]
struct RecoverSpan {
    span: HashMap<NonZeroU64, SpanRecords>,
    current: Option<(NonZeroU64, SpanRecords)>,
}
impl RecoverSpan {
    pub fn handle(&mut self, instruction: Instruction) {
        match instruction {
            Instruction::NewSpan { parent, span, name } => {
                assert!(self.current.is_none());
                let records = SpanRecords {
                    parent,
                    name: name.to_owned(),
                    records: Default::default(),
                };

                self.current = Some((span, records));
            }
            Instruction::FinishedSpan => {
                let (k, v) = self.current.take().unwrap();
                self.span.insert(k, v);
            }
            Instruction::NewRecord(span) => {
                assert!(self.current.is_none());
                self.current = Some(self.span.remove_entry(&span).unwrap());
            }
            Instruction::FinishedRecord => {
                let (k, v) = self.current.take().unwrap();
                self.span.insert(k, v);
            }
            Instruction::DeleteSpan(span) => {
                self.span.remove(&span);
            }
            _ => {}
        }
    }
}
