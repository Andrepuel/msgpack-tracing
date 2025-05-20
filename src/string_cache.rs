use crate::tape::{
    FieldValue, Instruction, InstructionId, InstructionSet, InstructionSetTrait, InstructionTrait,
    TapeMachine, Value,
};
use chrono::{DateTime, Utc};
use std::{collections::HashMap, num::NonZeroU64};
use tracing::Level;

#[derive(Clone, Copy, Debug)]
pub enum CacheInstruction<'a> {
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
    AddValue(FieldValue<'a, CacheString<'a>>),
    DeleteSpan(NonZeroU64),
}
impl InstructionTrait for CacheInstruction<'_> {
    fn id(self) -> InstructionId {
        match self {
            CacheInstruction::Restart => InstructionId::Restart,
            CacheInstruction::NewString(..) => InstructionId::NewString,
            CacheInstruction::NewSpan { .. } => InstructionId::NewSpan,
            CacheInstruction::FinishedSpan => InstructionId::FinishedSpan,
            CacheInstruction::NewRecord(..) => InstructionId::NewRecord,
            CacheInstruction::FinishedRecord => InstructionId::FinishedRecord,
            CacheInstruction::StartEvent { .. } => InstructionId::StartEvent,
            CacheInstruction::FinishedEvent => InstructionId::FinishedEvent,
            CacheInstruction::AddValue(..) => InstructionId::AddValue,
            CacheInstruction::DeleteSpan(..) => InstructionId::DeleteSpan,
        }
    }
}

pub struct CacheInstructionSet;
impl InstructionSetTrait for CacheInstructionSet {
    type Instruction<'a> = CacheInstruction<'a>;
}

#[derive(Clone, Copy, Debug)]
pub enum CacheString<'a> {
    Present(&'a str),
    Cached(u64),
}

pub struct StringCache<T> {
    forward: T,
    strings: HashMap<String, u64>,
}
impl<T> StringCache<T>
where
    T: TapeMachine<CacheInstructionSet>,
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
            self.forward.handle(CacheInstruction::NewString(string));
            self.strings.insert(string.to_owned(), id);
            CacheString::Cached(id)
        }
    }
}
impl<T> TapeMachine<InstructionSet> for StringCache<T>
where
    T: TapeMachine<CacheInstructionSet>,
{
    fn needs_restart(&mut self) -> bool {
        self.forward.needs_restart()
    }

    fn handle(&mut self, instruction: Instruction) {
        match instruction {
            Instruction::Restart => {
                self.strings.clear();
                self.forward.handle(CacheInstruction::Restart);
            }
            Instruction::NewSpan { parent, span, name } => {
                let name = self.cache_string(name);
                self.forward
                    .handle(CacheInstruction::NewSpan { parent, span, name });
            }
            Instruction::FinishedSpan => {
                self.forward.handle(CacheInstruction::FinishedSpan);
            }
            Instruction::NewRecord(span) => {
                self.forward.handle(CacheInstruction::NewRecord(span));
            }
            Instruction::FinishedRecord => {
                self.forward.handle(CacheInstruction::FinishedRecord);
            }
            Instruction::StartEvent {
                time,
                span,
                target,
                priority,
            } => {
                let target = self.cache_string(target);
                self.forward.handle(CacheInstruction::StartEvent {
                    time,
                    span,
                    target,
                    priority,
                });
            }
            Instruction::FinishedEvent => {
                self.forward.handle(CacheInstruction::FinishedEvent);
            }
            Instruction::AddValue(FieldValue { name, value }) => {
                let name = self.cache_string(name);
                let value = self.cache_value(value);
                self.forward
                    .handle(CacheInstruction::AddValue(FieldValue { name, value }));
            }
            Instruction::DeleteSpan(span) => {
                self.forward.handle(CacheInstruction::DeleteSpan(span));
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
    T: TapeMachine<InstructionSet>,
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
impl<T> TapeMachine<CacheInstructionSet> for StringUncache<T>
where
    T: TapeMachine<InstructionSet>,
{
    fn needs_restart(&mut self) -> bool {
        self.forward.needs_restart()
    }

    fn handle(&mut self, instruction: CacheInstruction) {
        match instruction {
            CacheInstruction::Restart => {
                self.forward.handle(Instruction::Restart);
            }
            CacheInstruction::NewString(str) => {
                self.strings.push(str.to_owned());
            }
            CacheInstruction::NewSpan { parent, span, name } => {
                let name = Self::uncache(&self.strings, name);
                self.forward
                    .handle(Instruction::NewSpan { parent, span, name });
            }
            CacheInstruction::FinishedSpan => {
                self.forward.handle(Instruction::FinishedSpan);
            }
            CacheInstruction::NewRecord(span) => {
                self.forward.handle(Instruction::NewRecord(span));
            }
            CacheInstruction::FinishedRecord => {
                self.forward.handle(Instruction::FinishedRecord);
            }
            CacheInstruction::StartEvent {
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
            CacheInstruction::FinishedEvent => {
                self.forward.handle(Instruction::FinishedEvent);
            }
            CacheInstruction::AddValue(FieldValue { name, value }) => {
                let name = Self::uncache(&self.strings, name);
                let value = Self::uncache_value(&self.strings, value);
                self.forward
                    .handle(Instruction::AddValue(FieldValue { name, value }));
            }
            CacheInstruction::DeleteSpan(span) => {
                self.forward.handle(Instruction::DeleteSpan(span));
            }
        }
    }
}
