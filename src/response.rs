use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter, WriteHalf};
use std::io::{Write, Cursor};
use tokio::net::TcpStream;

pub struct Response {
    sink : BufWriter<WriteHalf<TcpStream>>,
    nesting : Vec<i32>
}

impl Response {
    pub fn new(stream : WriteHalf<TcpStream>) -> Response {
       Response {
            sink: BufWriter::new(stream),
            nesting: Vec::new()
        }
    }

    pub async fn ok(&mut self) -> std::io::Result<()> {
        self.sink.write_all(b"+OK\r\n").await
    }

    pub async fn zero(&mut self) -> std::io::Result<()> {
        self.sink.write_all(b":0\r\n").await
    }

    pub async fn one(&mut self) -> std::io::Result<()> {
        self.sink.write_all(b":1\r\n").await
    }

    async fn write_decimal(&mut self, val: u64) -> std::io::Result<()> {
        let mut buf = [0u8; 12];
        let mut cursor = Cursor::new(&mut buf[..]);
        write!(&mut cursor, "{}", val)?;

        let pos = cursor.position() as usize;
        self.sink.write_all(&cursor.get_ref()[..pos]).await?;
        self.sink.write_all(b"\r\n").await?;

        Ok(())
    }

    pub async fn number(&mut self, number : i32) -> std::io::Result<()> {
        if number == 0 {
            self.zero().await?;
        } else if number == 1 {
            self.one().await?;
        } else {
            let mut buf = [0u8; 12];
            let mut buf = Cursor::new(&mut buf[..]);
            write!(&mut buf, "{}", number)?;
            let pos = buf.position() as usize;
            self.sink.write_u8(b':').await?;
            self.sink.write_all(b"\r\n").await?;
        }

        Ok(())
    }
}