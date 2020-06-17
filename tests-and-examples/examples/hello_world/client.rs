// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::executor::block_on;
use futures::prelude::*;
use futures::sink::SinkExt;
use grpcio::{ChannelBuilder, Environment, WriteFlags};
use grpcio_proto::example::helloworld::HelloRequest;
use grpcio_proto::example::helloworld_grpc::GreeterClient;
use std::thread::JoinHandle;

// server using same config 80000
static LOOPNUM: usize = 80000;
static SENDNUM: usize = 200;

fn main() {
    let env = Arc::new(Environment::new(1));
    let ch = ChannelBuilder::new(env).connect("localhost:50051");
    let client = GreeterClient::new(ch);

    println!("Wait a moment...");
    test_unary(&client);
    test_client_stream_send_split(&client);
    test_client_stream_send_all(&client);
    test_server_stream_send_all(&client);
}

fn test_unary(client: &GreeterClient) {
    let (tx, rx) = std::sync::mpsc::channel();
    let timer = create_time_thread("test_unary", rx);
    let mut req = HelloRequest::default();
    req.set_name("world".to_owned());

    for _ in 0..LOOPNUM {
        let _ = client.say_hello(&req).expect("rpc");
    }
    tx.send(()).unwrap();
    timer.join().unwrap();
}

fn test_client_stream_send_split(client: &GreeterClient) {
    let (tx, rx) = std::sync::mpsc::channel();
    let timer = create_time_thread("test_client_stream_send_split", rx);
    let mut req = HelloRequest::default();
    req.set_name("world".to_owned());

    let exec_test_f = async move {
        for _ in 0..LOOPNUM / SENDNUM {
            let (mut sink, receiver) = client.say_hello_client().unwrap();
            for _ in 0..SENDNUM {
                let req = req.clone();
                sink.send((req, WriteFlags::default())).await.unwrap();
            }
            sink.close().await.unwrap();
            let _ = receiver.await.unwrap();
        }
        tx.send(()).unwrap();
    };

    block_on(exec_test_f);
    timer.join().unwrap();
}

fn test_client_stream_send_all(client: &GreeterClient) {
    let (tx, rx) = std::sync::mpsc::channel();
    let timer = create_time_thread("test_client_stream_send_all", rx);
    let mut req = HelloRequest::default();
    req.set_name("world".to_owned());

    let exec_test_f = async move {
        for _ in 0..LOOPNUM / SENDNUM {
            let (mut sink, receiver) = client.say_hello_client().unwrap();
            let mut send_data = vec![];
            for _ in 0..SENDNUM {
                send_data.push(req.clone());
            }
            let send_stream = futures::stream::iter(send_data);
            sink.send_all(&mut send_stream.map(move |item| Ok((item, WriteFlags::default()))))
                .await
                .unwrap();
            sink.close().await.unwrap();
            let _ = receiver.await.unwrap();
        }
        tx.send(()).unwrap();
    };

    block_on(exec_test_f);
    timer.join().unwrap();
}

fn test_server_stream_send_all(client: &GreeterClient) {
    let (tx, rx) = std::sync::mpsc::channel();
    let timer = create_time_thread("test_server_stream_send_all", rx);

    let exec_test_f = async move {
        let mut req = HelloRequest::default();
        req.set_name("world".to_owned());
        let mut say_hello_server = client.say_hello_server(&req).unwrap();
        while let Some(_) = say_hello_server.try_next().await.unwrap() {}
        tx.send(()).unwrap();
    };

    block_on(exec_test_f);
    timer.join().unwrap();
}

fn create_time_thread(name: &str, rx: std::sync::mpsc::Receiver<()>) -> JoinHandle<()> {
    let name = name.to_owned();
    std::thread::spawn(move || {
        let start = std::time::Instant::now();
        let _ = rx.recv().unwrap();
        let use_time = start.elapsed().as_secs_f64();
        let qps = LOOPNUM as f64 / use_time;
        println!(
            "======== {} \nQPS: {} \nTime: {}\nNum: {}",
            name, qps, use_time, LOOPNUM
        );
    })
}
