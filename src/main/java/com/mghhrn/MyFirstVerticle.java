package com.mghhrn;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.*;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

public class MyFirstVerticle extends AbstractVerticle {

    private PgPool client;

    @Override
    public void start() {

        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(5432)
                .setHost("localhost")
                .setDatabase("mghtest")
                .setUser("postgres")
                .setPassword("")
                ;

        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(5)
                ;

        client = PgPool.pool(vertx, connectOptions, poolOptions);

        Router router = Router.router(vertx);

        router.get("/")
                .handler(routingContext -> routingContext.response().end("OK guys!"))
                .failureHandler(ErrorHandler.create(true));

        router.post("/product")
                .handler(ResponseTimeHandler.create())
                .handler(LoggerHandler.create(LoggerFormat.DEFAULT))
                .consumes("application/json")
                .handler(BodyHandler.create())
                .handler(this::addProduct)
                .failureHandler(ctx -> {
                    ctx.failure().printStackTrace();
                    ctx.response().setStatusCode(400).end("FAILURE!");
                });

        router.get("/product")
                .handler(ResponseTimeHandler.create())
                .handler(this::listProducts)
                .failureHandler(ctx -> {
                    ctx.failure().printStackTrace();
                    ctx.response().setStatusCode(400).end("FAILURE!");
                });


        vertx.createHttpServer()
                .requestHandler(router)
                .listen(8080);
    }


    @Override
    public void stop() throws Exception {
        super.stop();
        client.close();
        System.out.println("Bye!");
    }

    private void addProduct(RoutingContext rc) {

        JsonObject body = rc.getBodyAsJson();

        System.out.println(body.encodePrettily());

        Product product = new Product();
        product.setName(body.getString("name"));
        product.setPrice(body.getLong("price"));

        client.preparedQuery("INSERT INTO product (name , price) VALUES ($1 , $2) ",
                Tuple.of(product.getName(), product.getPrice()),
                ar -> {
                    if (ar.succeeded()) {
                        RowSet result = ar.result();
                        System.out.println("inserted value to DB.      row count = " + result.rowCount());
                        rc.response().end("Got " + result.size() + " rows ");
                    } else {
                        System.out.println("Failure: " + ar.cause().getMessage());
                        ar.cause().printStackTrace();
                        rc.response().setStatusCode(400).end("Failure: " + ar.cause().getMessage());
                    }
        });
    }

    private void listProducts(RoutingContext rc) {
        client.query("SELECT * FROM product",
                ar -> {
                    if (ar.succeeded()) {
                        RowSet result = ar.result();
                        JsonArray array = new JsonArray();
                        System.out.println("inserted value to DB.      row count = " + result.rowCount());
                        rc.response().end("Got " + result.size() + " rows ");
                    } else {
                        System.out.println("Failure: " + ar.cause().getMessage());
                        ar.cause().printStackTrace();
                        rc.response().setStatusCode(400).end("Failure: " + ar.cause().getMessage());
                    }
                });
    }
}
