use crate::tape::{
    FieldValueOwned, Instruction, InstructionSet, SpanRecords, TapeMachine, ValueOwned,
};
use chrono::{DateTime, Utc};
use nu_ansi_term::{Color, Style};
use std::borrow::Cow;
use std::fmt::Write;
use std::num::NonZeroU64;
use std::{collections::HashMap, io};
use tracing::Level;

pub struct Printer<W> {
    out: W,
    color: bool,
    span: HashMap<NonZeroU64, SpanRecords>,
    new_records: Option<(NonZeroU64, SpanRecords)>,
    new_event: Option<NewEvent>,
}
impl<W> Printer<W>
where
    W: io::Write + Send + 'static,
{
    pub fn new(out: W, color: bool) -> Self {
        Self {
            out,
            color,
            span: Default::default(),
            new_records: None,
            new_event: None,
        }
    }

    fn get_span(&self, span: NonZeroU64) -> Cow<SpanRecords> {
        match self.span.get(&span) {
            Some(span) => Cow::Borrowed(span),
            None => Cow::Owned(SpanRecords::lost(span)),
        }
    }

    fn take_span(&mut self, span: NonZeroU64) -> SpanRecords {
        match self.span.remove(&span) {
            Some(records) => records,
            None => SpanRecords::lost(span),
        }
    }

    fn span_iter<'a, F>(&'a self, span: NonZeroU64, f: &mut F)
    where
        F: FnMut(Cow<'a, SpanRecords>),
    {
        let records = self.get_span(span);
        if let Some(parent) = records.parent {
            self.span_iter(parent, f);
        }
        f(records);
    }

    fn span_from_root(&self, span: NonZeroU64) -> Vec<Cow<SpanRecords>> {
        let mut r = Vec::new();
        self.span_iter(span, &mut |records| {
            r.push(records);
        });
        r
    }
}
impl<W> TapeMachine<InstructionSet> for Printer<W>
where
    W: io::Write + Send + 'static,
{
    fn needs_restart(&mut self) -> bool {
        false
    }

    fn handle(&mut self, instruction: Instruction) {
        match instruction {
            Instruction::Restart => {}
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
                self.new_records = Some((id, self.take_span(id)));
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
                let spans = new_event
                    .span
                    .map(|span| self.span_from_root(span))
                    .unwrap_or_default();

                let line = new_event.to_line(self.color, &spans);

                let _ = self.out.write_all(line.as_bytes());
                let _ = self.out.write_all(b"\n");
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
    pub time: DateTime<Utc>,
    pub span: Option<NonZeroU64>,
    pub target: String,
    pub priority: Level,
    pub records: Vec<FieldValueOwned>,
}
impl NewEvent {
    pub fn to_line(&self, color: bool, spans: &[Cow<SpanRecords>]) -> String {
        let mut line = String::new();
        self.write_line(color, spans, &mut line);
        line
    }

    pub fn write_line<W>(&self, color: bool, spans: &[Cow<SpanRecords>], line: &mut W)
    where
        W: Write,
    {
        let dimmed = color.then(|| Style::new().dimmed());
        let bold = color.then(|| Style::new().bold());
        let level_color = color.then(|| Self::level_style(self.priority));
        let field_style = color.then(|| Style::new().italic());

        Self::with_style(dimmed, line, |line| write!(line, "{:?}", self.time)).unwrap();
        Self::with_style(level_color, line, |line| {
            write!(line, " {}", Self::level_padded(self.priority))
        })
        .unwrap();

        for (idx, span) in spans.iter().enumerate() {
            if idx == 0 {
                write!(line, " ").unwrap();
            }

            let name = &span.name;

            Self::with_style(bold, line, |line| write!(line, "{name}{{")).unwrap();

            for (idx, record) in span.records.iter().enumerate() {
                if idx > 0 {
                    write!(line, " ").unwrap();
                }
                Self::write_record(record, field_style, false, line).unwrap();
            }
            write!(line, "}}").unwrap();
            Self::with_style(dimmed, line, |line| write!(line, ":")).unwrap();
        }

        Self::with_style(dimmed, line, |line| write!(line, " {}:", self.target)).unwrap();

        for record in self.records.iter() {
            write!(line, " ").unwrap();
            Self::write_record(record, field_style, true, line).unwrap();
        }
    }

    fn level_style(level: Level) -> Style {
        match level {
            Level::TRACE => Color::Purple,
            Level::DEBUG => Color::Blue,
            Level::INFO => Color::Green,
            Level::WARN => Color::Yellow,
            Level::ERROR => Color::Red,
        }
        .normal()
    }

    fn level_padded(level: Level) -> &'static str {
        match level {
            Level::TRACE => "TRACE",
            Level::DEBUG => "DEBUG",
            Level::INFO => " INFO",
            Level::WARN => " WARN",
            Level::ERROR => "ERROR",
        }
    }

    fn write_record<W>(
        record: &FieldValueOwned,
        field_style: Option<Style>,
        with_message: bool,
        out: &mut W,
    ) -> std::fmt::Result
    where
        W: Write,
    {
        let name = &record.name;

        if name == "message" && with_message {
            if let ValueOwned::Debug(str) = &record.value {
                return write!(out, "{}", str);
            }
        }

        Self::with_style(field_style, out, |out| write!(out, "{name}"))?;

        write!(out, "=")?;
        Self::write_value(&record.value, out)
    }

    fn write_value<W>(value: &ValueOwned, out: &mut W) -> std::fmt::Result
    where
        W: Write,
    {
        match value {
            ValueOwned::Debug(str) => write!(out, "{str}"),
            ValueOwned::String(str) => write!(out, "{str:?}"),
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

    fn with_style<W, F>(style: Option<Style>, out: &mut W, f: F) -> std::fmt::Result
    where
        W: Write,
        F: FnOnce(&mut W) -> std::fmt::Result,
    {
        match style {
            Some(style) => {
                write!(out, "{}", style.prefix())?;
                f(out)?;
                write!(out, "{}", style.suffix())?;
                Ok(())
            }
            None => f(out),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn print_debug() {
        let event = NewEvent {
            time: Default::default(),
            span: None,
            target: "target".to_string(),
            priority: Level::INFO,
            records: vec![
                FieldValueOwned {
                    name: "dbg".to_string(),
                    value: ValueOwned::Debug("thing".to_string()),
                },
                FieldValueOwned {
                    name: "str".to_string(),
                    value: ValueOwned::String("thing".to_string()),
                },
            ],
        };

        assert_eq!(
            event.to_line(false, &[]),
            r#"1970-01-01T00:00:00Z  INFO target: dbg=thing str="thing""#
        );
    }

    #[test]
    fn log_levels_ident() {
        for (priority, str) in [
            (Level::ERROR, "ERROR"),
            (Level::WARN, " WARN"),
            (Level::INFO, " INFO"),
            (Level::DEBUG, "DEBUG"),
            (Level::TRACE, "TRACE"),
        ] {
            let event = NewEvent {
                time: Default::default(),
                span: None,
                target: "target".to_string(),
                priority,
                records: Default::default(),
            };

            assert_eq!(
                event.to_line(false, Default::default()),
                format!("1970-01-01T00:00:00Z {str} target:")
            )
        }
    }

    #[test]
    fn message_field_name_is_omitted() {
        let event = NewEvent {
            time: Default::default(),
            span: None,
            target: "target".to_string(),
            priority: Level::INFO,
            records: vec![FieldValueOwned {
                name: "message".to_string(),
                value: ValueOwned::Debug("a log".to_string()),
            }],
        };

        assert_eq!(
            event.to_line(false, Default::default()),
            "1970-01-01T00:00:00Z  INFO target: a log"
        )
    }

    #[test]
    fn span_print() {
        let event = NewEvent {
            time: Default::default(),
            span: None,
            target: "target".to_string(),
            priority: Level::INFO,
            records: Default::default(),
        };

        let spans = [
            SpanRecords {
                parent: None,
                name: "record".to_string(),
                records: vec![
                    FieldValueOwned {
                        name: "message".to_string(),
                        value: ValueOwned::String("a log".to_string()),
                    },
                    FieldValueOwned {
                        name: "a".to_string(),
                        value: ValueOwned::Debug("b".to_string()),
                    },
                ],
            },
            SpanRecords {
                parent: None,
                name: "second".to_string(),
                records: Default::default(),
            },
        ];
        let spans = spans.iter().map(Cow::Borrowed).collect::<Vec<_>>();

        assert_eq!(
            event.to_line(false, &spans),
            r#"1970-01-01T00:00:00Z  INFO record{message="a log" a=b}:second{}: target:"#
        );
    }
}
