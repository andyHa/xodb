mod request;


use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use bytes::{BytesMut, Buf, Bytes};
use std::error::Error;
use tokio::stream::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
#[macro_use]
use anyhow::{anyhow, Result};

use crate::request::Request;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = TcpListener::bind("127.0.0.1:6379").await?;
    let mut incoming = server.incoming();

    while let Some(Ok(stream)) = incoming.next().await {
        tokio::spawn(async move {
            if let Err(e) = process(stream).await {
                println!("failed to process connection; error = {}", e);
            }
        });
    }

    Ok(())
}

async fn process(stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut buffer = BytesMut::with_capacity(1024);
    let mut response = BufWriter::new(stream);
    loop {
        let bytes_read = response.read_buf(&mut buffer).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        if let Some(request) = Request::parse(&mut buffer).unwrap() {
            response.write_all(b"+OK\r\n").await?;
            response.flush().await?;

            if buffer.len() == 0 && buffer.capacity() > 1024 {
                buffer = BytesMut::with_capacity(1024);
            }
        }
    }

    Ok(())
}
//
// async fn respond(req: Frame) -> Result<Frame, Box<dyn Error>> {
//     if let Frame::Array(arr) = req {
//         Ok(Frame::Array(vec![Frame::SimpleString("OK".to_string()), Frame::Array(arr)]))
//     } else {
//         Ok(Frame::SimpleString("OK".to_string()))
//     }
// }
