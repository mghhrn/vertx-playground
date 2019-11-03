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
import io.vertx.sqlclient.Row;
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
                .handler(ResponseTimeHandler.create())
                .handler(ResponseContentTypeHandler.create())
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

    private void updateProduct(RoutingContext rc) {
        JsonObject jsonBody = rc.getBodyAsJson();
        Long givenId = Long.valueOf(rc.pathParam("id"));
        JsonObject productInDb = null;
        client.preparedQuery("select * from product where id = $1",
                Tuple.of(givenId),
                ar -> {
                    if (ar.succeeded()) {
                        if (ar.result().size() == 0)     //check the existence of the data logically
                            rc.response().setStatusCode(400).end("product not found!");

                        client.preparedQuery("update product set name = $1 , price = $2 where id = $3",
                                Tuple.of(jsonBody.getString("name"), jsonBody.getLong("price"), givenId),
                                ar2 -> {
                                    if (ar2.succeeded()) {
                                        rc.response().end("update completed!");
                                    }
                                    else {
                                        rc.response().setStatusCode(400).end("error executing update query!");
                                    }
                                });
                    }
                    else {
                        rc.response().setStatusCode(400).end("error in executing select query!");
                    }
                });
    }

    private void listProducts(RoutingContext rc) {
        client.query("SELECT * FROM product",
                ar -> {
                    if (ar.succeeded()) {
                        RowSet<Row> result = ar.result();
                        JsonArray array = new JsonArray();
                        for(Row row : result) {
                            array.add(new JsonObject()
                                    .put("id", row.getLong("id"))
                                    .put("name", row.getString("name"))
                                    .put("price", row.getLong("price"))
                            );
                        }
                        rc.response().end(array.encodePrettily());
                    } else {
                        System.out.println("Failure: " + ar.cause().getMessage());
                        ar.cause().printStackTrace();
                        rc.response().setStatusCode(400).end("Failure: " + ar.cause().getMessage());
                    }
                });
    }

    private void getOneProduct(RoutingContext rc) {
        Long productId = Long.valueOf(rc.pathParam("id"));
        client.preparedQuery("select name, price from product where id = $1",
                Tuple.of(productId),
                ar -> {
                    if(ar.succeeded()) {
                        Row row = ar.result().iterator().next();
                        rc.response().end(new JsonObject()
                            .put("id", productId)
                            .put("name", row.getString("name"))
                            .put("price", row.getLong("price"))
                            .encode());
                    }
                    else {
                        ar.cause().printStackTrace();
                        rc.response().setStatusCode(400).end("fail inside!");
                    }
                });
    }

    private void deleteProduct(RoutingContext rc) {
        Long productId = Long.valueOf(rc.pathParam("id"));
        client.preparedQuery("select * from product where id = $1",
                Tuple.of(productId),
                ar -> {
                    if (ar.succeeded()) {
                        if (ar.result().size() == 0)     //check the existence of the data logically
                            rc.response().setStatusCode(400).end("product with id " + productId + " was not found!");

                        client.preparedQuery("delete from product where id = $1",
                                Tuple.of(productId),
                                ar2 -> {
                                    if (ar2.succeeded()) {
                                        rc.response().end("delete completed!");
                                    }
                                    else {
                                        ar2.cause().printStackTrace();
                                        rc.response().setStatusCode(400).end("error executing delete query!");
                                    }
                                });
                    }
                    else {
                        rc.response().setStatusCode(400).end("error in executing select query!");
                    }
                });
    }
}
