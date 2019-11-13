package com.mghhrn;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.http.HttpServer;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.handler.BodyHandler;
import io.vertx.rxjava.ext.web.handler.LoggerHandler;
import io.vertx.rxjava.ext.web.handler.ResponseContentTypeHandler;
import io.vertx.rxjava.ext.web.handler.ResponseTimeHandler;
import io.vertx.rxjava.pgclient.PgPool;
import io.vertx.rxjava.sqlclient.Row;
import io.vertx.rxjava.sqlclient.RowSet;
import io.vertx.rxjava.sqlclient.Tuple;
import io.vertx.sqlclient.PoolOptions;
import rx.Single;


public class MyFirstRXVerticle extends AbstractVerticle {

    private PgPool client;

    @Override
    public void start() {
        HttpServer server = vertx.createHttpServer();

        //we can pass some config to the Context of verticle using "--conf" config
        System.out.println("Context config= " + config().encodePrettily());
        processArgs().forEach(arg -> System.out.println("process args = " + arg));
        System.out.println("verticle id = " + this.deploymentID());

        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(config().getInteger("db.port"))
                .setHost(config().getString("db.host"))
                .setDatabase(config().getString("db.name"))
                .setUser(config().getString("db.user"))
                .setPassword(config().getString("db.password"))
                ;

        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(5)
                ;

        client = PgPool.pool(vertx, connectOptions, poolOptions);

        // rx version
        Router router = Router.router(vertx);

        router.post("/product")
                .handler(ResponseTimeHandler.create())
                .handler(ResponseContentTypeHandler.create())
                .handler(LoggerHandler.create(LoggerFormat.DEFAULT))
                .consumes("application/json")
                .handler(BodyHandler.create())
                .handler(this::addProduct)
                .failureHandler(ctx -> {
                    ctx.failure().printStackTrace();
                    ctx.response().setStatusCode(400).end("FAILURE!");
                });

        router.put("/product/:id")
                .handler(ResponseTimeHandler.create())
                .handler(ResponseContentTypeHandler.create())
                .handler(LoggerHandler.create(LoggerFormat.DEFAULT))
                .consumes("application/json")
                .handler(BodyHandler.create())
                .handler(this::updateProduct)
                .failureHandler(ctx -> {
                    ctx.failure().printStackTrace();
                    ctx.response().setStatusCode(400).end("FAILURE!");
                });

        router.delete("/product/:id")
                .handler(ResponseTimeHandler.create())
                .handler(ResponseContentTypeHandler.create())
                .handler(LoggerHandler.create(LoggerFormat.DEFAULT))
                .handler(this::deleteProduct)
                .failureHandler(ctx -> {
                    ctx.failure().printStackTrace();
                    ctx.response().setStatusCode(400).end("FAILURE!");
                });

        router.get("/product")
                .handler(ResponseTimeHandler.create())  // rx version
                .handler(ResponseContentTypeHandler.create())  // rx version
                .produces("application/json")
                .handler(this::listProducts)
                .failureHandler(ctx -> {
                    ctx.failure().printStackTrace();
                    ctx.response().setStatusCode(400).end("FAILURE!");
                });

        router.get("/product/:id")
                .handler(ResponseTimeHandler.create())
                .handler(ResponseContentTypeHandler.create())
                .produces("application/json")
                .handler(this::getOneProduct)
                .failureHandler(ctx -> {
                    ctx.failure().printStackTrace();
                    ctx.response().setStatusCode(400).end("FAILURE!");
                });

        // We get the stream of request as Observable
        server.requestStream()
                .toObservable()
                .subscribe(router::handle);

        // We start the server using rxListen returning a
        // Single of HTTP server. We need to subscribe to
        // trigger the operation
        Single<HttpServer> serverSingle = server.rxListen(8080);
        serverSingle.subscribe(successfullyCreatedServer -> System.out.println(successfullyCreatedServer.toString()),
                error -> System.out.println("Failed to start HttpServer!"));
    }


    /*************** PRIVATE HANDLERS ***************/

    // rx version for RoutingContext
    private void addProduct(RoutingContext rc) {
        JsonObject body = rc.getBodyAsJson();
        Product product = new Product();
        product.setName(body.getString("name"));
        product.setPrice(body.getLong("price"));

        Single<RowSet<Row>> rowSetSingle = client.rxPreparedQuery("INSERT INTO product (name , price) VALUES ($1 , $2) ",
            Tuple.of(product.getName(), product.getPrice()));
        rowSetSingle.subscribe(result -> {
            System.out.println("inserted value to DB.      row count = " + result.rowCount());
            rc.response().end("Got " + result.size() + " rows ");
        }, error -> {
            System.out.println("Failure: " + error.getMessage());
            error.printStackTrace();
            rc.response().setStatusCode(400).end("Failure: " + error.getMessage());
        });
    }


    private void updateProduct(RoutingContext rc) {
        JsonObject jsonBody = rc.getBodyAsJson();
        Long givenId = Long.valueOf(rc.pathParam("id"));
        JsonObject productInDb = null;

        Single<RowSet<Row>> rowSetSingle = client.rxPreparedQuery("select * from product where id = $1",
                Tuple.of(givenId));
        rowSetSingle.subscribe(result -> {
                if (result.size() == 0)     //check the existence of the data logically
                    rc.response().setStatusCode(400).end("product not found!");
                Single<RowSet<Row>> rowSetOfUpdateSingle = client.rxPreparedQuery(
                        "update product set name = $1 , price = $2 where id = $3",
                        Tuple.of(jsonBody.getString("name"), jsonBody.getLong("price"), givenId)
                );
                rowSetOfUpdateSingle.subscribe(
                        success -> rc.response().end("update completed!"),
                        error -> rc.response().setStatusCode(400).end("error executing update query!")
                );
            }, error -> rc.response().setStatusCode(400).end("error in executing select query!")
        );
    }


    private void listProducts(RoutingContext rc) {

        Single<RowSet<Row>> single = client.rxQuery("SELECT * FROM product");
        single.subscribe(result -> {
            JsonArray array = new JsonArray();
            for(Row row : result) {
                array.add(new JsonObject()
                        .put("id", row.getLong("id"))
                        .put("name", row.getString("name"))
                        .put("price", row.getLong("price"))
                );
            }
            rc.response().end(array.encodePrettily());
        }, error -> {
            System.out.println("Failure: " + error.getMessage());
            error.printStackTrace();
            rc.response().setStatusCode(400).end("Failure: " + error.getMessage());
        });
    }

    private void getOneProduct(RoutingContext rc) {
        Long productId = Long.valueOf(rc.pathParam("id"));
        Single<RowSet<Row>> selectRowSetSingle = client.rxPreparedQuery("select name, price from product where id = $1",
                Tuple.of(productId));
        selectRowSetSingle.subscribe(result -> {
            Row row = result.iterator().next();
            rc.response().end(new JsonObject()
                    .put("id", productId)
                    .put("name", row.getString("name"))
                    .put("price", row.getLong("price"))
                    .encode());
        }, error -> {
            error.printStackTrace();
            rc.response().setStatusCode(400).end("Fail to get product!");
        });
    }


    private void deleteProduct(RoutingContext rc) {
        Long productId = Long.valueOf(rc.pathParam("id"));
        Single<RowSet<Row>> selectRowSetSingle = client.rxPreparedQuery("select * from product where id = $1",
                Tuple.of(productId));

        selectRowSetSingle.subscribe(selectResult -> {
            if (selectResult.size() == 0)     //check the existence of the data logically
                rc.response().setStatusCode(400).end("product with id " + productId + " was not found!");

            Single<RowSet<Row>> deleteRowSetSingle = client.rxPreparedQuery("delete from product where id = $1",
                    Tuple.of(productId));
            deleteRowSetSingle.subscribe(
                    success -> rc.response().end("delete completed!"),
                    error -> rc.response().setStatusCode(400).end("error executing delete query!")
            );

        }, error -> rc.response().setStatusCode(400).end("error in executing select query!"));
    }
}