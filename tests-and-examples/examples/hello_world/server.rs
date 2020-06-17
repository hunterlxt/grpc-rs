// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate log;

#[path = "../log_util.rs"]
mod log_util;

use std::io::Read;
use std::sync::Arc;
use std::{io, thread};

use futures::channel::oneshot;
use futures::executor::block_on;
use futures::prelude::*;
use grpcio::{
    ClientStreamingSink, Environment, RequestStream, RpcContext, ServerBuilder,
    ServerStreamingSink, UnarySink, WriteFlags,
};

use grpcio_proto::example::helloworld::{HelloReply, HelloRequest};
use grpcio_proto::example::helloworld_grpc::{create_greeter, Greeter};

// client using same config 80000
static LOOPNUM: usize = 80000;
static SENDNUM: usize = 200;

#[derive(Clone)]
struct GreeterService;

impl Greeter for GreeterService {
    fn say_hello(&mut self, ctx: RpcContext<'_>, req: HelloRequest, sink: UnarySink<HelloReply>) {
        let msg = format!("Hello {}", req.get_name());
        let mut resp = HelloReply::default();
        resp.set_message(msg);
        let f = sink
            .success(resp)
            .map_err(move |e| error!("failed to reply {:?}: {:?}", req, e))
            .map(|_| ());
        ctx.spawn(f)
    }

    fn say_hello_client(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: RequestStream<HelloRequest>,
        sink: ClientStreamingSink<HelloReply>,
    ) {
        // recv SENDNUM and return at once
        let f = async move {
            let mut msg = String::new();
            while let Some(req) = req.try_next().await? {
                msg = req.get_name().to_owned();
            }
            let mut resp = HelloReply::default();
            let msg = format!("Hello {}", msg);
            resp.set_message(msg);
            sink.success(resp).await?;
            Ok(())
        }
        .map_err(|e: grpcio::Error| error!("failed to resp: {:?}", e))
        .map(|_| ());

        ctx.spawn(f)
    }

    fn say_hello_server(
        &mut self,
        ctx: RpcContext<'_>,
        req: HelloRequest,
        mut sink: ServerStreamingSink<HelloReply>,
    ) {
        let msg = format!("Hello {}", req.get_name());
        let mut resp = HelloReply::default();
        resp.set_message(msg);
        let mut send_data = vec![];
        for _ in 0..SENDNUM {
            send_data.push(resp.clone());
        }

        let f = async move {
            for _ in 0..LOOPNUM / SENDNUM {
                let send_data = send_data.clone();
                let send_stream = futures::stream::iter(send_data);
                sink.send_all(&mut send_stream.map(move |item| Ok((item, WriteFlags::default()))))
                    .await
                    .unwrap();
            }
            sink.close().await.unwrap();
            Ok(())
        }
        .map_err(|e: grpcio::Error| error!("failed to resp: {:?}", e))
        .map(|_| ());

        ctx.spawn(f)
    }
}

fn main() {
    let _guard = log_util::init_log(None);
    let env = Arc::new(Environment::new(1));
    let service = create_greeter(GreeterService);

    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind("127.0.0.1", 50_051)
        .build()
        .unwrap();
    server.start();
    for (host, port) in server.bind_addrs() {
        info!("listening on {}:{}", host, port);
    }
    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        info!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = block_on(rx);
    let _ = block_on(server.shutdown());
}
