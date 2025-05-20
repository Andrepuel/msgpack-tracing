use chrono::{DateTime, Utc};
use std::{
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

pub fn install<T>(machine: T)
where
    T: TapeMachine<InstructionSet>,
{
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

pub trait TapeMachine<I>: Send + 'static
where
    I: InstructionSetTrait,
{
    fn needs_restart(&mut self) -> bool;
    fn handle(&mut self, instruction: I::Instruction<'_>);
}

pub trait InstructionSetTrait {
    type Instruction<'a>: InstructionTrait;
}
pub struct InstructionSet;
impl InstructionSetTrait for InstructionSet {
    type Instruction<'a> = Instruction<'a>;
}

pub trait InstructionTrait: Copy {
    fn id(self) -> InstructionId;
}

#[derive(Clone, Copy, Debug)]
pub enum Instruction<'a> {
    Restart,
    NewSpan {
        parent: Option<NonZeroU64>,
        span: NonZeroU64,
        name: &'a str,
    },
    FinishedSpan,
    NewRecord(NonZeroU64),
    FinishedRecord,
    StartEvent {
        time: DateTime<Utc>,
        span: Option<NonZeroU64>,
        target: &'a str,
        priority: Level,
    },
    FinishedEvent,
    AddValue(FieldValue<'a, &'a str>),
    DeleteSpan(NonZeroU64),
}
impl InstructionTrait for Instruction<'_> {
    fn id(self) -> InstructionId {
        match self {
            Instruction::Restart => InstructionId::Restart,
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

#[derive(Clone, Copy, Debug)]
pub struct FieldValue<'a, S> {
    pub name: S,
    pub value: Value<'a, S>,
}
impl<'a> FieldValue<'a, &'a str> {
    pub fn to_owned(self) -> FieldValueOwned {
        FieldValueOwned {
            name: self.name.to_owned(),
            value: self.value.to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct FieldValueOwned {
    pub name: String,
    pub value: ValueOwned,
}
impl FieldValueOwned {
    pub fn as_ref(&self) -> FieldValue<&str> {
        FieldValue {
            name: &self.name,
            value: self.value.as_ref(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Value<'a, S> {
    Debug(S),
    String(S),
    Float(f64),
    Integer(i64),
    Unsigned(u64),
    Bool(bool),
    ByteArray(&'a [u8]),
}
impl<S> From<f64> for Value<'_, S> {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}
impl<S> From<i64> for Value<'_, S> {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}
impl<S> From<u64> for Value<'_, S> {
    fn from(value: u64) -> Self {
        Self::Unsigned(value)
    }
}
impl<S> From<bool> for Value<'_, S> {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}
impl<'a, S> From<&'a [u8]> for Value<'a, S> {
    fn from(value: &'a [u8]) -> Self {
        Value::ByteArray(value)
    }
}
impl<'a> Value<'a, &'a str> {
    fn to_owned(self) -> ValueOwned {
        match self {
            Value::Debug(str) => ValueOwned::Debug(str.to_owned()),
            Value::String(str) => ValueOwned::String(str.to_owned()),
            Value::Float(value) => ValueOwned::Float(value),
            Value::Integer(value) => ValueOwned::Integer(value),
            Value::Unsigned(value) => ValueOwned::Unsigned(value),
            Value::Bool(value) => ValueOwned::Bool(value),
            Value::ByteArray(items) => ValueOwned::ByteArray(items.to_owned()),
        }
    }
}

#[derive(Clone)]
pub enum ValueOwned {
    Debug(String),
    String(String),
    Float(f64),
    Integer(i64),
    Unsigned(u64),
    Bool(bool),
    ByteArray(Vec<u8>),
}
impl ValueOwned {
    pub fn as_ref(&self) -> Value<&str> {
        match self {
            ValueOwned::Debug(value) => Value::Debug(value),
            ValueOwned::String(value) => Value::String(value),
            ValueOwned::Float(value) => Value::Float(*value),
            ValueOwned::Integer(value) => Value::Integer(*value),
            ValueOwned::Unsigned(value) => Value::Unsigned(*value),
            ValueOwned::Bool(value) => Value::Bool(*value),
            ValueOwned::ByteArray(items) => Value::ByteArray(items),
        }
    }
}

pub struct TapeMachineLogger<T> {
    inner: Mutex<TapeMachineLoggerInner<T>>,
}
impl<T> TapeMachineLogger<T>
where
    T: TapeMachine<InstructionSet>,
{
    pub fn new(mut machine: T) -> Self {
        machine.handle(Instruction::Restart);
        TapeMachineLogger {
            inner: Mutex::new(TapeMachineLoggerInner { machine }),
        }
    }

    fn machine(&self) -> MutexGuard<'_, TapeMachineLoggerInner<T>> {
        let mut machine = self.inner.lock().unwrap();
        if machine.machine.needs_restart() {
            machine.handle(Instruction::Restart);
        }
        machine
    }
}
impl<T, S> Layer<S> for TapeMachineLogger<T>
where
    T: TapeMachine<InstructionSet>,
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut machine = self.machine();
        let name = attrs.metadata().name();
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
        let target = event.metadata().target();
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
}
impl<T> TapeMachineLoggerInner<T>
where
    T: TapeMachine<InstructionSet>,
{
    fn field_value<'a, V>(&mut self, field: &Field, value: V) -> FieldValue<'a, &'a str>
    where
        V: Into<Value<'a, &'a str>>,
    {
        let name = field.name();
        let value = value.into();

        FieldValue { name, value }
    }

    fn handle(&mut self, instruction: Instruction) {
        self.machine.handle(instruction);
    }
}

struct VisitMachine<'a, T>(&'a mut TapeMachineLoggerInner<T>);
impl<T> Visit for VisitMachine<'_, T>
where
    T: TapeMachine<InstructionSet>,
{
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let value = format!("{value:?}");
        let value = self.0.field_value(field, Value::Debug(value.as_str()));
        self.0.handle(Instruction::AddValue(value));
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
        let value = self.0.field_value(field, Value::String(value));
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

#[derive(Clone)]
pub struct SpanRecords {
    pub parent: Option<NonZeroU64>,
    pub name: String,
    pub records: Vec<FieldValueOwned>,
}
impl SpanRecords {
    pub fn lost(span: NonZeroU64) -> Self {
        Self {
            parent: None,
            name: format!("span-{span}"),
            records: Default::default(),
        }
    }
}
