package com.mghhrn;

import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.http.HttpServer;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public class MyFirstRXVerticle extends AbstractVerticle {
    @Override
    public void start() {
        HttpServer server = vertx.createHttpServer();

        // We get the stream of request as Observable
        server.requestStream().toObservable()
                .subscribe(req -> {
                    // for each HTTP request, this method is called
                    req.response().end("Hello from RXJAVA "
                            + Thread.currentThread().getName());
                });

        // We start the server using rxListen returning a
        // Single of HTTP server. We need to subscribe to
        // trigger the operation
        server
                .rxListen(8080)
                .subscribe();
    }


}