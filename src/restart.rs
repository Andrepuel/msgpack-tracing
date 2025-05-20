use crate::tape::{Instruction, InstructionSet, SpanRecords, TapeMachine};
use std::{collections::HashMap, num::NonZeroU64};

pub struct RestartableMachine<T> {
    forward: T,
    span: HashMap<NonZeroU64, SpanRecords>,
    current_span: Option<(NonZeroU64, SpanRecords)>,
}
impl<T> RestartableMachine<T>
where
    T: TapeMachine<InstructionSet>,
{
    pub fn new(forward: T) -> Self {
        Self {
            forward,
            span: Default::default(),
            current_span: None,
        }
    }
}
impl<T> TapeMachine<InstructionSet> for RestartableMachine<T>
where
    T: TapeMachine<InstructionSet>,
{
    fn needs_restart(&mut self) -> bool {
        self.forward.needs_restart()
    }

    fn handle(&mut self, instruction: Instruction) {
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
