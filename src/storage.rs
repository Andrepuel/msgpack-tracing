use crate::{
    string_cache::{CacheInstruction, CacheInstructionSet, CacheString},
    tape::{
        FieldValue, Instruction, InstructionId, InstructionSet, InstructionTrait, TapeMachine,
        Value,
    },
};
use chrono::DateTime;
use rmp::{Marker, decode, encode};
use std::{
    io::{self, BufRead, BufReader, Read},
    num::NonZeroU64,
};
use tracing::Level;

pub struct Store<W>(W);
impl<W> Store<W>
where
    W: io::Write + Send + 'static,
{
    pub fn new(out: W) -> Self {
        Self(out)
    }

    pub fn do_handle(write: &mut W, instruction: Instruction) -> io::Result<()> {
        let instruction = match instruction {
            Instruction::Restart => CacheInstruction::Restart,
            Instruction::NewSpan { parent, span, name } => {
                let name = CacheString::Present(name);
                CacheInstruction::NewSpan { parent, span, name }
            }
            Instruction::FinishedSpan => CacheInstruction::FinishedSpan,
            Instruction::NewRecord(span) => CacheInstruction::NewRecord(span),
            Instruction::FinishedRecord => CacheInstruction::FinishedRecord,
            Instruction::StartEvent {
                time,
                span,
                target,
                priority,
            } => {
                let target = CacheString::Present(target);
                CacheInstruction::StartEvent {
                    time,
                    span,
                    target,
                    priority,
                }
            }
            Instruction::FinishedEvent => CacheInstruction::FinishedEvent,
            Instruction::AddValue(FieldValue { name, value }) => {
                let name = CacheString::Present(name);
                let value = match value {
                    Value::Debug(str) => Value::String(CacheString::Present(str)),
                    Value::String(str) => Value::String(CacheString::Present(str)),
                    Value::Float(data) => Value::Float(data),
                    Value::Integer(data) => Value::Integer(data),
                    Value::Unsigned(data) => Value::Unsigned(data),
                    Value::Bool(data) => Value::Bool(data),
                    Value::ByteArray(items) => Value::ByteArray(items),
                };

                CacheInstruction::AddValue(FieldValue { name, value })
            }
            Instruction::DeleteSpan(span) => CacheInstruction::DeleteSpan(span),
        };

        Self::do_handle_cached(write, instruction)
    }

    pub fn do_handle_cached(write: &mut W, instruction: CacheInstruction) -> io::Result<()> {
        write.write_all(&[instruction.id().into()])?;
        match instruction {
            CacheInstruction::Restart => (),
            CacheInstruction::NewString(data) => encode::write_str(write, data)?,
            CacheInstruction::NewSpan { parent, span, name } => {
                let parent = parent.map(Into::into).unwrap_or(0);
                let span = span.into();
                encode::write_uint(write, parent)?;
                encode::write_uint(write, span)?;
                Self::write_cache_str(write, name)?;
            }
            CacheInstruction::FinishedSpan => (),
            CacheInstruction::NewRecord(span) => {
                let span: u64 = span.into();
                encode::write_uint(write, span)?;
            }
            CacheInstruction::FinishedRecord => (),
            CacheInstruction::StartEvent {
                time,
                span,
                target,
                priority,
            } => {
                let time2 = time.timestamp_subsec_nanos();
                let time = time.timestamp() as u64;
                let span = span.map(Into::into).unwrap_or(0);
                let priority = priority_num(priority);

                encode::write_uint(write, time)?;
                encode::write_uint(write, time2 as u64)?;
                encode::write_uint(write, span)?;
                Self::write_cache_str(write, target)?;
                encode::write_uint(write, priority)?;
            }
            CacheInstruction::FinishedEvent => (),
            CacheInstruction::AddValue(field_value) => {
                Self::write_cache_str(write, field_value.name)?;
                Self::write_cache_value(write, field_value.value)?;
            }
            CacheInstruction::DeleteSpan(span) => {
                let span = span.into();
                encode::write_uint(write, span)?;
            }
        }

        Ok(())
    }

    fn write_cache_str(write: &mut W, str: CacheString) -> io::Result<()> {
        match str {
            CacheString::Present(data) => encode::write_str(write, data)?,
            CacheString::Cached(index) => {
                CacheIndex::from(index).write(write)?;
            }
        }

        Ok(())
    }

    fn write_cache_value(write: &mut W, value: Value<CacheString>) -> io::Result<()> {
        match value {
            Value::Debug(str) => {
                encode::write_array_len(write, 1)?;
                Self::write_cache_str(write, str)?;
            }
            Value::String(str) => Self::write_cache_str(write, str)?,
            Value::Float(data) => encode::write_f64(write, data)?,
            Value::Integer(data) => {
                encode::write_sint(write, data)?;
            }
            Value::Unsigned(data) => {
                encode::write_uint(write, data)?;
            }
            Value::Bool(data) => encode::write_bool(write, data)?,
            Value::ByteArray(data) => encode::write_bin(write, data)?,
        }

        Ok(())
    }
}
impl<W> TapeMachine<CacheInstructionSet> for Store<W>
where
    W: io::Write + Send + 'static,
{
    fn needs_restart(&mut self) -> bool {
        false
    }

    fn handle(&mut self, instruction: CacheInstruction) {
        let _ = Self::do_handle_cached(&mut self.0, instruction);
    }
}
impl<W> TapeMachine<InstructionSet> for Store<W>
where
    W: io::Write + Send + 'static,
{
    fn needs_restart(&mut self) -> bool {
        false
    }

    fn handle(&mut self, instruction: Instruction) {
        let _ = Self::do_handle(&mut self.0, instruction);
    }
}

pub struct Load<R> {
    read: BufReader<R>,
    buf1: Vec<u8>,
    buf2: Vec<u8>,
    started: bool,
}
impl<R> Load<R>
where
    R: io::Read,
{
    pub fn new(input: R) -> Self {
        Self {
            read: BufReader::new(input),
            buf1: Default::default(),
            buf2: Default::default(),
            started: false,
        }
    }

    pub fn forward<T>(&mut self, machine: &mut T) -> io::Result<()>
    where
        T: TapeMachine<InstructionSet>,
    {
        while let Some(instruction) = self.fetch_one()? {
            machine.handle(instruction);
        }

        Ok(())
    }

    pub fn forward_cached<T>(&mut self, machine: &mut T) -> io::Result<()>
    where
        T: TapeMachine<CacheInstructionSet>,
    {
        while let Some(instruction) = self.fetch_one_cached()? {
            machine.handle(instruction);
        }

        Ok(())
    }

    pub fn fetch_one(&mut self) -> io::Result<Option<Instruction>> {
        let Some(instruction) = self.fetch_one_cached()? else {
            return Ok(None);
        };

        Ok(Some(match instruction {
            CacheInstruction::Restart => Instruction::Restart,
            CacheInstruction::NewString(_) => return Err(UnexpectedCached.into()),
            CacheInstruction::NewSpan { parent, span, name } => {
                let name = match name {
                    CacheString::Present(str) => str,
                    CacheString::Cached(_) => return Err(UnexpectedCached.into()),
                };

                Instruction::NewSpan { parent, span, name }
            }
            CacheInstruction::FinishedSpan => Instruction::FinishedSpan,
            CacheInstruction::NewRecord(span) => Instruction::NewRecord(span),
            CacheInstruction::FinishedRecord => Instruction::FinishedRecord,
            CacheInstruction::StartEvent {
                time,
                span,
                target,
                priority,
            } => {
                let target = match target {
                    CacheString::Present(str) => str,
                    CacheString::Cached(_) => return Err(UnexpectedCached.into()),
                };

                Instruction::StartEvent {
                    time,
                    span,
                    target,
                    priority,
                }
            }
            CacheInstruction::FinishedEvent => Instruction::FinishedEvent,
            CacheInstruction::AddValue(FieldValue { name, value }) => {
                let name = match name {
                    CacheString::Present(str) => str,
                    CacheString::Cached(_) => return Err(UnexpectedCached.into()),
                };
                let value = match value {
                    Value::Debug(CacheString::Present(str)) => Value::Debug(str),
                    Value::Debug(CacheString::Cached(_)) => return Err(UnexpectedCached.into()),
                    Value::String(CacheString::Present(str)) => Value::String(str),
                    Value::String(CacheString::Cached(_)) => return Err(UnexpectedCached.into()),
                    Value::Float(value) => Value::Float(value),
                    Value::Integer(value) => Value::Integer(value),
                    Value::Unsigned(value) => Value::Unsigned(value),
                    Value::Bool(value) => Value::Bool(value),
                    Value::ByteArray(items) => Value::ByteArray(items),
                };

                Instruction::AddValue(FieldValue { name, value })
            }
            CacheInstruction::DeleteSpan(span) => Instruction::DeleteSpan(span),
        }))
    }

    pub fn fetch_one_cached(&mut self) -> io::Result<Option<CacheInstruction>> {
        let instruction = loop {
            let Some(instruction) = self.read.fill_buf()?.first().copied() else {
                return Ok(None);
            };
            self.read.consume(1);

            if self.started {
                break instruction;
            }

            if instruction == u8::from(InstructionId::Restart) {
                self.started = true;
            }
        };

        let instruction = InstructionId::try_from(instruction).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("bad instruction {e}"))
        })?;

        Ok(Some(match instruction {
            InstructionId::Restart => CacheInstruction::Restart,
            InstructionId::NewString => CacheInstruction::NewString(self.read_str()?),
            InstructionId::NewSpan => {
                let parent: u64 = decode::read_int(&mut self.read).map_err(decode_err)?;
                let span: u64 = decode::read_int(&mut self.read).map_err(decode_err)?;
                let name = self.read_cache_str()?;

                CacheInstruction::NewSpan {
                    parent: NonZeroU64::new(parent),
                    span: NonZeroU64::new(span).ok_or(ZeroSpan)?,
                    name,
                }
            }
            InstructionId::FinishedSpan => CacheInstruction::FinishedSpan,
            InstructionId::NewRecord => {
                let span = decode::read_int(&mut self.read).map_err(decode_err)?;

                CacheInstruction::NewRecord(NonZeroU64::new(span).ok_or(ZeroSpan)?)
            }
            InstructionId::FinishedRecord => CacheInstruction::FinishedRecord,
            InstructionId::StartEvent => {
                let time: u64 = decode::read_int(&mut self.read).map_err(decode_err)?;
                let time2: u64 = decode::read_int(&mut self.read).map_err(decode_err)?;
                let span = decode::read_int(&mut self.read).map_err(decode_err)?;
                let target = Self::do_read_cache_str(&mut self.read, &mut self.buf1)?;
                let priority = num_priority(decode::read_int(&mut self.read).map_err(decode_err)?);

                CacheInstruction::StartEvent {
                    time: DateTime::from_timestamp(time as i64, time2 as u32).unwrap_or_default(),
                    span: NonZeroU64::new(span),
                    target,
                    priority,
                }
            }
            InstructionId::FinishedEvent => CacheInstruction::FinishedEvent,
            InstructionId::AddValue => {
                let name = Self::do_read_cache_str(&mut self.read, &mut self.buf1)?;
                let value = Self::do_read_value(&mut self.read, &mut self.buf2)?;

                CacheInstruction::AddValue(FieldValue { name, value })
            }
            InstructionId::DeleteSpan => {
                let span: u64 = decode::read_int(&mut self.read).map_err(decode_err)?;
                CacheInstruction::DeleteSpan(NonZeroU64::new(span).ok_or(ZeroSpan)?)
            }
        }))
    }

    fn read_str(&mut self) -> io::Result<&str> {
        Self::do_read_str(&mut self.read, &mut self.buf1)
    }

    fn do_read_str<'a>(read: &mut BufReader<R>, buf: &'a mut Vec<u8>) -> io::Result<&'a str> {
        let len = decode::read_str_len(read).map_err(decode_err)?;
        buf.resize(len as usize, 0);
        read.read_exact(buf.as_mut_slice())?;

        std::str::from_utf8(buf.as_slice()).map_err(decode_err)
    }

    fn do_read_value<'a>(
        read: &mut BufReader<R>,
        buf: &'a mut Vec<u8>,
    ) -> io::Result<Value<'a, CacheString<'a>>> {
        Ok(match Self::do_peek_marker(read)? {
            Marker::FixArray(1) => {
                read.consume(1);
                Value::Debug(Self::do_read_cache_str(read, buf)?)
            }
            Marker::FixPos(_)
            | Marker::FixNeg(_)
            | Marker::I8
            | Marker::I16
            | Marker::I32
            | Marker::I64 => Value::Integer(decode::read_int(read).map_err(decode_err)?),
            Marker::FixStr(_)
            | Marker::Str8
            | Marker::Str16
            | Marker::Str32
            | Marker::FixExt1
            | Marker::FixExt2
            | Marker::FixExt4
            | Marker::FixExt8 => Value::String(Self::do_read_cache_str(read, buf)?),
            Marker::False => Value::Bool(false),
            Marker::True => Value::Bool(true),
            Marker::Bin8 | Marker::Bin16 | Marker::Bin32 => {
                let n = decode::read_bin_len(read).map_err(decode_err)?;
                buf.resize(n as usize, 0);
                read.read_exact(buf)?;
                Value::ByteArray(buf)
            }
            Marker::F32 => Value::Float(decode::read_f32(read).map_err(decode_err)? as f64),
            Marker::F64 => Value::Float(decode::read_f64(read).map_err(decode_err)?),
            Marker::U8 | Marker::U16 | Marker::U32 | Marker::U64 => {
                Value::Unsigned(decode::read_int(read).map_err(decode_err)?)
            }
            marker => return Err(UnexpectedMarker(marker).into()),
        })
    }

    fn read_cache_str(&mut self) -> io::Result<CacheString> {
        Self::do_read_cache_str(&mut self.read, &mut self.buf1)
    }

    fn do_read_cache_str<'a>(
        read: &mut BufReader<R>,
        buf: &'a mut Vec<u8>,
    ) -> io::Result<CacheString<'a>> {
        Ok(match Self::do_peek_marker(read)? {
            Marker::FixStr(_) | Marker::Str8 | Marker::Str16 | Marker::Str32 => {
                CacheString::Present(Self::do_read_str(read, buf)?)
            }
            Marker::FixExt1 | Marker::FixExt2 | Marker::FixExt4 | Marker::FixExt8 => {
                CacheString::Cached(CacheIndex::read(read)?.into())
            }
            marker => return Err(UnexpectedMarker(marker).into()),
        })
    }

    fn do_peek_marker(read: &mut BufReader<R>) -> io::Result<Marker> {
        let marker = read.fill_buf()?.first().ok_or(EofOnMarker)?;

        Ok(Marker::from_u8(*marker))
    }
}

pub fn priority_num(level: Level) -> u64 {
    match level {
        Level::TRACE => 0,
        Level::DEBUG => 1,
        Level::INFO => 2,
        Level::WARN => 3,
        Level::ERROR => 4,
    }
}

pub fn num_priority(num: u64) -> Level {
    match num {
        0 => Level::TRACE,
        1 => Level::DEBUG,
        2 => Level::INFO,
        3 => Level::WARN,
        4 => Level::ERROR,
        _ => Level::ERROR,
    }
}

fn decode_err<E: ToString>(error: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

#[derive(thiserror::Error, Debug)]
#[error("Unexpected type {0:?}")]
pub struct UnexpectedMarker(Marker);
impl From<UnexpectedMarker> for io::Error {
    fn from(value: UnexpectedMarker) -> Self {
        decode_err(value)
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Expecting Msgpack Marker, got EOF")]
pub struct EofOnMarker;
impl From<EofOnMarker> for io::Error {
    fn from(value: EofOnMarker) -> Self {
        decode_err(value)
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Span should not have value of zero")]
pub struct ZeroSpan;
impl From<ZeroSpan> for io::Error {
    fn from(value: ZeroSpan) -> Self {
        decode_err(value)
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Trying to load cached instruction file into uncached machine")]
pub struct UnexpectedCached;
impl From<UnexpectedCached> for io::Error {
    fn from(value: UnexpectedCached) -> Self {
        decode_err(value)
    }
}

#[derive(Clone, Copy)]
pub enum CacheIndex {
    U16 { data: [u8; 2] },
    U24 { data: [u8; 3] },
    U40 { data: [u8; 5] },
    U64 { data: [u8; 9] },
}
impl From<CacheIndex> for u64 {
    fn from(value: CacheIndex) -> Self {
        match value {
            CacheIndex::U16 { data } => u64::from_le_bytes([data[0], data[1], 0, 0, 0, 0, 0, 0]),
            CacheIndex::U24 { data } => {
                u64::from_le_bytes([data[0], data[1], data[2], 0, 0, 0, 0, 0])
            }
            CacheIndex::U40 { data } => {
                u64::from_le_bytes([data[0], data[1], data[2], data[3], data[4], 0, 0, 0])
            }
            CacheIndex::U64 { data } => u64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]),
        }
    }
}
impl From<u64> for CacheIndex {
    fn from(value: u64) -> Self {
        let bytes = value.to_le_bytes();
        match bytes {
            [data0, data1, 0, 0, 0, 0, 0, 0] => CacheIndex::U16 {
                data: [data0, data1],
            },
            [data0, data1, data2, 0, 0, 0, 0, 0] => CacheIndex::U24 {
                data: [data0, data1, data2],
            },
            [data0, data1, data2, data3, data4, 0, 0, 0] => CacheIndex::U40 {
                data: [data0, data1, data2, data3, data4],
            },
            data => CacheIndex::U64 {
                data: [
                    0, data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ],
            },
        }
    }
}
impl CacheIndex {
    pub fn marker(self) -> Marker {
        match self {
            CacheIndex::U16 { .. } => Marker::FixExt1,
            CacheIndex::U24 { .. } => Marker::FixExt2,
            CacheIndex::U40 { .. } => Marker::FixExt4,
            CacheIndex::U64 { .. } => Marker::FixExt8,
        }
    }

    pub fn data(&self) -> &[u8] {
        match self {
            CacheIndex::U16 { data } => data.as_slice(),
            CacheIndex::U24 { data } => data.as_slice(),
            CacheIndex::U40 { data } => data.as_slice(),
            CacheIndex::U64 { data } => data.as_slice(),
        }
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        match self {
            CacheIndex::U16 { data } => data.as_mut_slice(),
            CacheIndex::U24 { data } => data.as_mut_slice(),
            CacheIndex::U40 { data } => data.as_mut_slice(),
            CacheIndex::U64 { data } => data.as_mut_slice(),
        }
    }

    pub fn write<W>(self, mut write: W) -> io::Result<()>
    where
        W: io::Write,
    {
        write.write_all(&[self.marker().to_u8()])?;
        write.write_all(self.data())?;
        Ok(())
    }

    pub fn read<R>(mut read: R) -> io::Result<Self>
    where
        R: io::Read,
    {
        let mut marker = [0];
        read.read_exact(&mut marker)?;
        let marker = Marker::from_u8(marker[0]);

        let mut r = match marker {
            Marker::FixExt1 => CacheIndex::U16 {
                data: Default::default(),
            },
            Marker::FixExt2 => CacheIndex::U24 {
                data: Default::default(),
            },
            Marker::FixExt4 => CacheIndex::U40 {
                data: Default::default(),
            },
            Marker::FixExt8 => CacheIndex::U64 {
                data: Default::default(),
            },
            marker => return Err(UnexpectedMarker(marker).into()),
        };

        read.read_exact(r.data_mut())?;

        Ok(r)
    }
}
