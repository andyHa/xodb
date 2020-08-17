use bytes::{Bytes, BytesMut, Buf};
#[macro_use]
use anyhow::{anyhow, Result, Context};
use std::fmt;

#[derive(Copy, Clone, Debug)]
struct Range {
    start: usize,
    end: usize,
}

impl Range {
    fn next_offset(&self) -> usize {
        self.end + 3
    }
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

pub struct Request {
    data: Bytes,
    command: Range,
    arguments: Vec<Range>
}


const DOLLAR: u8 = b'$';
const ASTERISK: u8 = b'*';
const CR: u8 = b'\r';
const DOT: u8 = b'.';
const ZERO_DIGIT: u8 = b'0';
const NINE_DIGIT: u8 = b'9';

fn read_int(buffer: &BytesMut, offset: usize) -> anyhow::Result<Option<(i32, Range)>> {
    let mut len: i32 = 0;
    let mut index = offset;
    while index < buffer.len() {
        let digit = buffer[index];
        if digit == CR {
            return if buffer.len() >= index + 1 {
                Ok(Some((len, Range { start: offset, end: index - 1 })))
            } else {
                Ok(None)
            };
        }
        if digit < ZERO_DIGIT || digit > NINE_DIGIT {
            return Err(anyhow!("Malformed integer at position {}", index));
        }

        len = len * 10 + (digit - ZERO_DIGIT) as i32;
        index += 1;
    }

    Ok(None)
}

fn read_bulk_string(buffer: &BytesMut, offset: usize) -> anyhow::Result<Option<Range>> {
    if offset >= buffer.len() {
        return Ok(None);
    }
    if buffer[offset] != DOLLAR {
        return Err(anyhow!("Expected a bulk string at {}", offset));
    }

    if let Some((length, range)) = read_int(buffer, offset + 1)? {
        let next_offset = range.next_offset();
        if buffer.len() >= next_offset + length as usize + 2 {
            return Ok(Some(Range { start: next_offset, end: next_offset + length as usize - 1 }));
        }
    }

    Ok(None)
}


impl Request {
    pub fn parse(data: &mut BytesMut) -> anyhow::Result<Option<Request>> {
        if data.is_empty() {
            return Ok(None);
        }

        let mut offset = 0;
        if data[0] != ASTERISK {
            dbg!(data[0]);
            return Err(anyhow!("A request must be an array of bulk strings!"));
        } else {
            offset += 1;
        }

        let (mut num_args, range) = match read_int(&data, offset)? {
            Some((num_args, range)) => (num_args - 1, range),
            _ => return Ok(None)
        };
        offset = range.next_offset();

        let command = match read_bulk_string(&data, offset)? {
            Some(range) => range,
            _ => return Ok(None)
        };
        offset = command.next_offset();

        let mut arguments = Vec::with_capacity(num_args as usize);
        while num_args > 0 {
            if let Some(range) = read_bulk_string(&data, offset)? {
                arguments.push(range);
                num_args -= 1;
                offset = range.next_offset();
            } else {
                return Ok(None);
            }
        }

        if offset >= data.len() {
            data.clear();
        } else {
            data.advance(offset);
        }

        Ok(Some(Request {
            data: data.to_bytes(),
            command,
            arguments,
        }))
    }

    pub fn command(&self) -> &str {
        std::str::from_utf8(&self.data[self.command.start..=self.command.end]).unwrap()
    }

    pub fn parameter_count(&self) -> usize {
        self.arguments.len()
    }

    pub fn parameter(&self, index: usize) -> Result<Bytes> {
        if index < self.arguments.len() {
            Ok(self.data.slice(self.arguments[index].start..=self.arguments[index].end))
        } else {
            Err(anyhow!("Invalid parameter index {} (only {} are present)", index, self.arguments.len()))
        }
    }

    pub fn str_parameter(&self, index: usize) -> Result<&str> {
        if index < self.arguments.len() {
            let range = self.arguments[index];
            std::str::from_utf8(&self.data[range.start..=range.end])
                .with_context(|| format!("Failed to parse parameter {} (range {}) as UTF-8 string!", index, range))
        } else {
            Err(anyhow!("Invalid parameter index {} (only {} are present)", index, self.arguments.len()))
        }
    }

    pub fn int_parameter(&self, index: usize) -> Result<i32> {
        let string = self.str_parameter(index)?;
        string.parse().with_context(|| format!("Failed to parse parameter {} ('{}') as integer!", index, string))
    }

}

#[cfg(test)]
mod tests {
    use crate::request::Request;
    use bytes::{Bytes, BytesMut};

    #[test]
    fn a_command_is_successfully_parsed() {
        let request = Request::parse(&mut BytesMut::from("*2\r\n$10\r\ntest.hello\r\n$5\r\nWorld\r\n")).unwrap().unwrap();
        assert_eq!(request.parameter_count(), 1);
        assert_eq!(request.command(), "test.hello");
        assert_eq!(request.str_parameter(0).unwrap(), "World");
    }

    #[test]
    fn missing_array_is_detected() {
        let result = Request::parse(&mut BytesMut::from("+GET"));
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn non_bulk_string_is_detected() {
        let result = Request::parse(&mut BytesMut::from("*1\r\n+GET"));
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn invalid_number_is_detected() {
        let result = Request::parse(&mut BytesMut::from("*GET\r\n"));
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn an_incomplete_command_is_skipped() {
        {
            let result = Request::parse(&mut BytesMut::from("")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result = Request::parse(&mut BytesMut::from("*")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result = Request::parse(&mut BytesMut::from("*1")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result = Request::parse(&mut BytesMut::from("*1\r")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result = Request::parse(&mut BytesMut::from("*1\r\n")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result = Request::parse(&mut BytesMut::from("*2\r\n$10\r\ntest.h")).unwrap();
            assert_eq!(result.is_none(), true);
        }
        {
            let result = Request::parse(&mut BytesMut::from("*2\r\n$10\r\ntest.hello\r\n")).unwrap();
            assert_eq!(result.is_none(), true);
        }
    }
}