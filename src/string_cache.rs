use crate::tape::{
    CacheString, FieldValue, Instruction, InstructionCachedRef, InstructionRef, SpanRecords,
    TapeMachine, Value,
};
use std::{collections::HashMap, num::NonZeroU64};

pub struct StringCache<T> {
    forward: T,
    strings: HashMap<String, u64>,
}
impl<T> StringCache<T>
where
    T: TapeMachine<InstructionCachedRef>,
{
    pub fn new(forward: T) -> Self {
        Self {
            forward,
            strings: Default::default(),
        }
    }

    fn cache_value<'a>(&mut self, value: Value<'a, &'a str>) -> Value<'a, CacheString<'a>> {
        match value {
            Value::String(string) => Value::String(self.cache_string(string)),
            Value::Float(value) => Value::Float(value),
            Value::Integer(value) => Value::Integer(value),
            Value::Unsigned(value) => Value::Unsigned(value),
            Value::Bool(value) => Value::Bool(value),
            Value::ByteArray(value) => Value::ByteArray(value),
        }
    }

    fn cache_string<'a>(&mut self, string: &'a str) -> CacheString<'a> {
        if let Some(id) = self.strings.get(string) {
            return CacheString::Cached(*id);
        }

        let id = self.strings.len() as u64;
        let small = !matches!(
            (id, string.len()),
            (0..=0xffff, 4..)
                | (0x1_0000..=0xff_ffff, 5..)
                | (0x100_0000..=0xff_ffff_ffff, 7..)
                | (_, 11..)
        );

        if small {
            CacheString::Present(string)
        } else {
            self.forward.handle(Instruction::NewString(string));
            self.strings.insert(string.to_owned(), id);
            CacheString::Cached(id)
        }
    }
}
impl<T> TapeMachine<InstructionRef> for StringCache<T>
where
    T: TapeMachine<InstructionCachedRef>,
{
    fn needs_restart(&mut self) -> bool {
        self.forward.needs_restart()
    }

    fn handle(&mut self, instruction: Instruction<&str>) {
        match instruction {
            Instruction::Restart => {
                self.strings.clear();
                self.forward.handle(Instruction::Restart);
            }
            Instruction::NewString(str) => {
                let new_id = self.strings.len() as u64;
                self.strings.insert(str.to_owned(), new_id);
                self.forward.handle(Instruction::NewString(str));
            }
            Instruction::NewSpan { parent, span, name } => {
                let name = self.cache_string(name);
                self.forward
                    .handle(Instruction::NewSpan { parent, span, name });
            }
            Instruction::FinishedSpan => {
                self.forward.handle(Instruction::FinishedSpan);
            }
            Instruction::NewRecord(span) => {
                self.forward.handle(Instruction::NewRecord(span));
            }
            Instruction::FinishedRecord => {
                self.forward.handle(Instruction::FinishedRecord);
            }
            Instruction::StartEvent {
                time,
                span,
                target,
                priority,
            } => {
                let target = self.cache_string(target);
                self.forward.handle(Instruction::StartEvent {
                    time,
                    span,
                    target,
                    priority,
                });
            }
            Instruction::FinishedEvent => {
                self.forward.handle(Instruction::FinishedEvent);
            }
            Instruction::AddValue(FieldValue { name, value }) => {
                let name = self.cache_string(name);
                let value = self.cache_value(value);
                self.forward
                    .handle(Instruction::AddValue(FieldValue { name, value }));
            }
            Instruction::DeleteSpan(span) => {
                self.forward.handle(Instruction::DeleteSpan(span));
            }
        }
    }
}

pub struct RestartableMachine<T> {
    forward: T,
    span: HashMap<NonZeroU64, SpanRecords>,
    current_span: Option<(NonZeroU64, SpanRecords)>,
}
impl<T> RestartableMachine<T>
where
    T: TapeMachine<InstructionRef>,
{
    pub fn new(forward: T) -> Self {
        Self {
            forward,
            span: Default::default(),
            current_span: None,
        }
    }
}
impl<T> TapeMachine<InstructionRef> for RestartableMachine<T>
where
    T: TapeMachine<InstructionRef>,
{
    fn needs_restart(&mut self) -> bool {
        self.forward.needs_restart()
    }

    fn handle(&mut self, instruction: Instruction<&str>) {
        match instruction {
            Instruction::Restart => {
                self.forward.handle(Instruction::Restart);

                for (span, records) in self.span.iter() {
                    self.forward.handle(Instruction::NewSpan {
                        parent: records.parent,
                        span: *span,
                        name: records.name.as_ref(),
                    });

                    for record in records.records.iter() {
                        self.forward.handle(Instruction::AddValue(record.as_ref()));
                    }

                    self.forward.handle(Instruction::FinishedSpan);
                }
            }
            Instruction::NewString(str) => {
                self.forward.handle(Instruction::NewString(str));
            }
            Instruction::NewSpan { parent, span, name } => {
                assert!(self.current_span.is_none());
                self.current_span = Some((
                    span,
                    SpanRecords {
                        parent,
                        name: name.to_owned(),
                        records: Default::default(),
                    },
                ));

                self.forward
                    .handle(Instruction::NewSpan { parent, span, name });
            }
            Instruction::FinishedSpan => {
                let (k, v) = self.current_span.take().unwrap();
                self.span.insert(k, v);
                self.forward.handle(Instruction::FinishedSpan)
            }
            Instruction::NewRecord(span) => {
                assert!(self.current_span.is_none());
                self.current_span = Some(self.span.remove_entry(&span).unwrap());
                self.forward.handle(Instruction::NewRecord(span));
            }
            Instruction::FinishedRecord => {
                let (k, v) = self.current_span.take().unwrap();
                self.span.insert(k, v);
                self.forward.handle(Instruction::FinishedRecord)
            }
            Instruction::StartEvent {
                time,
                span,
                target,
                priority,
            } => {
                self.forward.handle(Instruction::StartEvent {
                    time,
                    span,
                    target,
                    priority,
                });
            }
            Instruction::FinishedEvent => self.forward.handle(Instruction::FinishedEvent),
            Instruction::AddValue(field_value) => {
                if let Some((_, current_span)) = self.current_span.as_mut() {
                    current_span.records.push(field_value.to_owned());
                }
                self.forward.handle(Instruction::AddValue(field_value));
            }
            Instruction::DeleteSpan(span) => {
                self.span.remove(&span);
                self.forward.handle(Instruction::DeleteSpan(span));
            }
        }
    }
}

pub struct StringUncache<T> {
    forward: T,
    strings: Vec<String>,
}
impl<T> StringUncache<T>
where
    T: TapeMachine<InstructionRef>,
{
    pub fn new(forward: T) -> Self {
        Self {
            forward,
            strings: Default::default(),
        }
    }

    fn uncache<'a>(strings: &'a [String], string: CacheString<'a>) -> &'a str {
        match string {
            CacheString::Present(str) => str,
            CacheString::Cached(index) => strings[index as usize].as_str(),
        }
    }

    fn uncache_value<'a>(
        strings: &'a [String],
        value: Value<'a, CacheString<'a>>,
    ) -> Value<'a, &'a str> {
        match value {
            Value::String(string) => Value::String(Self::uncache(strings, string)),
            Value::Float(value) => Value::Float(value),
            Value::Integer(value) => Value::Integer(value),
            Value::Unsigned(value) => Value::Unsigned(value),
            Value::Bool(value) => Value::Bool(value),
            Value::ByteArray(items) => Value::ByteArray(items),
        }
    }
}
impl<T> TapeMachine<InstructionCachedRef> for StringUncache<T>
where
    T: TapeMachine<InstructionRef>,
{
    fn needs_restart(&mut self) -> bool {
        self.forward.needs_restart()
    }

    fn handle(&mut self, instruction: Instruction<CacheString>) {
        match instruction {
            Instruction::Restart => {
                self.forward.handle(Instruction::Restart);
            }
            Instruction::NewString(str) => {
                self.strings.push(str.to_owned());
            }
            Instruction::NewSpan { parent, span, name } => {
                let name = Self::uncache(&self.strings, name);
                self.forward
                    .handle(Instruction::NewSpan { parent, span, name });
            }
            Instruction::FinishedSpan => {
                self.forward.handle(Instruction::FinishedSpan);
            }
            Instruction::NewRecord(span) => {
                self.forward.handle(Instruction::NewRecord(span));
            }
            Instruction::FinishedRecord => {
                self.forward.handle(Instruction::FinishedRecord);
            }
            Instruction::StartEvent {
                time,
                span,
                target,
                priority,
            } => {
                let target = Self::uncache(&self.strings, target);

                self.forward.handle(Instruction::StartEvent {
                    time,
                    span,
                    target,
                    priority,
                });
            }
            Instruction::FinishedEvent => {
                self.forward.handle(Instruction::FinishedEvent);
            }
            Instruction::AddValue(FieldValue { name, value }) => {
                let name = Self::uncache(&self.strings, name);
                let value = Self::uncache_value(&self.strings, value);
                self.forward
                    .handle(Instruction::AddValue(FieldValue { name, value }));
            }
            Instruction::DeleteSpan(span) => {
                self.forward.handle(Instruction::DeleteSpan(span));
            }
        }
    }
}
