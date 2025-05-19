use crate::tape::{CacheStringOwned, FieldValueOwned, Instruction, TapeMachine, ValueOwned};
use chrono::{DateTime, Utc};
use std::borrow::Cow;
use std::fmt::Write;
use std::{collections::HashMap, io};
use tracing::Level;

pub struct Printer<W> {
    out: W,
    strings: Vec<String>,
    span: HashMap<u64, SpanRecords>,
    new_records: Option<(u64, SpanRecords)>,
    new_event: Option<NewEvent>,
}
impl<W> Printer<W>
where
    W: io::Write + Send + 'static,
{
    pub fn new(out: W) -> Self {
        Self {
            out,
            strings: Default::default(),
            span: Default::default(),
            new_records: None,
            new_event: None,
        }
    }

    fn get_str<'a>(&'a self, str: &'a CacheStringOwned) -> &'a str {
        match str {
            CacheStringOwned::Small(small) => small.as_str(),
            CacheStringOwned::Cached(index) => self.strings.get(*index as usize).unwrap(),
        }
    }

    fn write_value<'a>(&'a self, value: &'a ValueOwned) -> Cow<'a, str> {
        match value {
            ValueOwned::String(str) => Cow::Borrowed(self.get_str(str)),
            ValueOwned::Float(value) => Cow::Owned(value.to_string()),
            ValueOwned::Integer(value) => Cow::Owned(value.to_string()),
            ValueOwned::Unsigned(value) => Cow::Owned(value.to_string()),
            ValueOwned::Bool(value) => Cow::Owned(value.to_string()),
            ValueOwned::ByteArray(items) => {
                let value =
                    items
                        .iter()
                        .fold(String::with_capacity(items.len() * 2), |mut a, b| {
                            write!(a, "{:02x}", *b).unwrap();
                            a
                        });
                Cow::Owned(value)
            }
        }
    }

    fn span_iter<F>(&self, span: u64, f: &mut F)
    where
        F: FnMut(&SpanRecords),
    {
        let records = self.span.get(&span).unwrap();
        if let Some(parent) = records.parent {
            self.span_iter(parent, f);
        }
        f(records);
    }
}
impl<W> TapeMachine for Printer<W>
where
    W: io::Write + Send + 'static,
{
    fn handle(&mut self, instruction: Instruction) {
        match instruction {
            Instruction::NewString(string) => self.strings.push(string.to_owned()),
            Instruction::NewSpan { parent, span, name } => {
                assert!(self.new_records.is_none());
                self.new_records = Some((
                    span,
                    SpanRecords {
                        parent,
                        name: name.to_owned(),
                        records: Default::default(),
                    },
                ));
            }
            Instruction::FinishedSpan | Instruction::FinishedRecord => {
                let new = self.new_records.take().unwrap();
                self.span.insert(new.0, new.1);
            }
            Instruction::NewRecord(id) => {
                assert!(self.new_records.is_none());
                self.new_records = Some((id, self.span.remove(&id).unwrap()));
            }
            Instruction::StartEvent {
                time,
                span,
                target,
                priority,
            } => {
                assert!(self.new_event.is_none());
                self.new_event = Some(NewEvent {
                    time,
                    span,
                    target: target.to_owned(),
                    priority,
                    records: Default::default(),
                });
            }
            Instruction::FinishedEvent => {
                let new_event = self.new_event.take().unwrap();
                let mut line = String::new();
                write!(line, "{:?} {} ", new_event.time, new_event.priority).unwrap();
                write!(line, "{}", self.get_str(&new_event.target)).unwrap();

                if let Some(span) = new_event.span {
                    self.span_iter(span, &mut |span| {
                        let name = self.get_str(&span.name);
                        write!(line, ":{name}{{").unwrap();
                        for (idx, record) in span.records.iter().enumerate() {
                            if idx > 0 {
                                write!(line, " ").unwrap();
                            }
                            let field = self.get_str(&record.name);
                            let value = self.write_value(&record.value);
                            write!(line, "{field}={value}").unwrap();
                        }
                        write!(line, "}}").unwrap();
                    });
                }

                for record in new_event.records.iter() {
                    let field = self.get_str(&record.name);
                    let value = self.write_value(&record.value);
                    if field == "message" {
                        write!(line, " {value}").unwrap();
                    } else {
                        write!(line, " {field}={value}").unwrap();
                    }
                }

                writeln!(line).unwrap();
                let _ = self.out.write_all(line.as_bytes());
            }
            Instruction::AddValue(field_value) => {
                match (&mut self.new_records, &mut self.new_event) {
                    (Some(new_records), None) => {
                        new_records.1.records.push(field_value.to_owned());
                    }
                    (None, Some(new_event)) => {
                        new_event.records.push(field_value.to_owned());
                    }
                    _ => panic!(),
                }
            }
            Instruction::DeleteSpan(id) => {
                self.span.remove(&id);
            }
        }
    }
}

pub struct SpanRecords {
    parent: Option<u64>,
    name: CacheStringOwned,
    records: Vec<FieldValueOwned>,
}

pub struct NewEvent {
    time: DateTime<Utc>,
    span: Option<u64>,
    target: CacheStringOwned,
    priority: Level,
    records: Vec<FieldValueOwned>,
}
