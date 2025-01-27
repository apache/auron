// Copyright 2022 The Blaze Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(feature = "jemalloc-pprof")]
mod memory_profiling;
#[cfg(feature = "jemalloc-pprof")]
mod pprof;

use std::sync::Mutex;

use once_cell::sync::OnceCell;
use poem::{listener::TcpListener, Route, RouteMethod, Server};

pub static HTTP_SERVICE: OnceCell<HttpService> = OnceCell::new();

pub trait Handler {
    fn get_route_method(&self) -> RouteMethod;
    fn get_route_path(&self) -> String;
}

pub trait HTTPServer: Send + Sync {
    fn start(&self);
    fn register_handler(&self, handler: impl Handler + 'static);
}

pub struct DefaultHTTPServer {
    runtime: tokio::runtime::Runtime,
    handlers: Mutex<Vec<Box<dyn Handler>>>,
}

unsafe impl Send for DefaultHTTPServer {}
unsafe impl Sync for DefaultHTTPServer {}

impl DefaultHTTPServer {
    pub fn new() -> Self {
        Self {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_io()
                .build()
                .unwrap(),
            handlers: Mutex::new(vec![]),
        }
    }
}

fn find_available_port() -> Option<u16> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|listener| listener.local_addr().ok().map(|addr| addr.port()))
}

impl HTTPServer for DefaultHTTPServer {
    fn start(&self) {
        if let Some(port) = find_available_port() {
            let mut app = Route::new();
            let handlers = self.handlers.lock().unwrap();
            for handler in handlers.iter() {
                app = app.at(handler.get_route_path(), handler.get_route_method());
            }
            self.runtime.spawn(async move {
                let _ = Server::new(TcpListener::bind(format!("0.0.0.0:{}", port)))
                    .name("blaze-native-http-service")
                    .run(app)
                    .await;
            });
            eprintln!("Blaze http service started. port: {}", port);
        } else {
            eprintln!("Failed to find an available port and http service is disabled!")
        }
    }

    fn register_handler(&self, handler: impl Handler + 'static) {
        let mut handlers = self.handlers.lock().unwrap();
        handlers.push(Box::new(handler));
    }
}

pub struct HttpService;

impl HttpService {
    pub fn init() -> Self {
        let server = DefaultHTTPServer::new();
        #[cfg(feature = "jemalloc-pprof")]
        {
            use crate::http::pprof::PProfHandler;
            server.register_handler(PProfHandler::default());

            use crate::http::memory_profiling::MemoryProfileHandler;
            server.register_handler(MemoryProfileHandler::default());
        }
        server.start();
        Self
    }
}
