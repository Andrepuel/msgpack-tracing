use crate::tape::{
    CacheStringOwned, FieldValueOwned, Instruction, SpanRecords, TapeMachine, ValueOwned,
};
use chrono::{DateTime, Utc};
use nu_ansi_term::{Color, Style};
use std::fmt::Write;
use std::num::NonZeroU64;
use std::{collections::HashMap, io};
use tracing::Level;

pub struct Printer<W> {
    out: W,
    strings: Vec<String>,
    span: HashMap<NonZeroU64, SpanRecords>,
    new_records: Option<(NonZeroU64, SpanRecords)>,
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

    fn write_value<O>(&self, value: &ValueOwned, mut out: O) -> std::fmt::Result
    where
        O: Write,
    {
        match value {
            ValueOwned::String(str) => write!(out, "{:?}", self.get_str(str)),
            ValueOwned::Float(value) => write!(out, "{value}"),
            ValueOwned::Integer(value) => write!(out, "{value}"),
            ValueOwned::Unsigned(value) => write!(out, "{value}"),
            ValueOwned::Bool(value) => write!(out, "{value}"),
            ValueOwned::ByteArray(items) => {
                for &char in items.iter() {
                    write!(out, "{char:02x}")?;
                }
                Ok(())
            }
        }
    }

    fn span_iter<F>(&self, span: NonZeroU64, f: &mut F)
    where
        F: FnMut(&SpanRecords),
    {
        let records = self.span.get(&span).unwrap();
        if let Some(parent) = records.parent {
            self.span_iter(parent, f);
        }
        f(records);
    }

    fn level_style(level: Level) -> Color {
        match level {
            Level::TRACE => Color::Purple,
            Level::DEBUG => Color::Blue,
            Level::INFO => Color::Green,
            Level::WARN => Color::Yellow,
            Level::ERROR => Color::Red,
        }
    }

    fn write_record<O>(
        &self,
        record: &FieldValueOwned,
        with_message: bool,
        mut out: O,
    ) -> std::fmt::Result
    where
        O: Write,
    {
        let name = self.get_str(&record.name);

        if name == "message" && with_message {
            if let ValueOwned::String(str) = &record.value {
                return write!(out, "{}", self.get_str(str));
            }
        }

        let italic = Style::new().italic();
        write!(out, "{}{name}{}=", italic.prefix(), italic.suffix())?;
        self.write_value(&record.value, out)
    }
}
impl<W> TapeMachine for Printer<W>
where
    W: io::Write + Send + 'static,
{
    fn needs_restart(&mut self) -> bool {
        false
    }

    fn handle(&mut self, instruction: Instruction) {
        match instruction {
            Instruction::Restart => {
                self.strings.clear();
            }
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
                let dimmed = Style::new().dimmed();
                let bold = Style::new().bold();

                let new_event = self.new_event.take().unwrap();
                let mut line = String::new();
                write!(line, "{}", dimmed.prefix()).unwrap();
                write!(line, "{:?}", new_event.time).unwrap();
                write!(line, "{}", dimmed.suffix()).unwrap();

                let level_color = Self::level_style(new_event.priority);
                write!(
                    line,
                    "  {}{}{} ",
                    level_color.prefix(),
                    new_event.priority,
                    level_color.suffix()
                )
                .unwrap();

                if let Some(span) = new_event.span {
                    self.span_iter(span, &mut |span| {
                        let name = self.get_str(&span.name);

                        write!(line, "{}{name}{{{}", bold.prefix(), bold.suffix()).unwrap();
                        for (idx, record) in span.records.iter().enumerate() {
                            if idx > 0 {
                                write!(line, " ").unwrap();
                            }
                            self.write_record(record, false, &mut line).unwrap();
                        }
                        write!(line, "}}").unwrap();
                        write!(line, "{}", dimmed.paint(":")).unwrap();
                    });
                }

                write!(
                    line,
                    " {}{}:{}",
                    dimmed.prefix(),
                    self.get_str(&new_event.target),
                    dimmed.suffix()
                )
                .unwrap();

                for record in new_event.records.iter() {
                    write!(line, " ").unwrap();
                    self.write_record(record, true, &mut line).unwrap();
                }

                writeln!(line).unwrap();
                let _ = self.out.write_all(line.as_bytes());
                let _ = self.out.flush();
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

pub struct NewEvent {
    time: DateTime<Utc>,
    span: Option<NonZeroU64>,
    target: CacheStringOwned,
    priority: Level,
    records: Vec<FieldValueOwned>,
}
