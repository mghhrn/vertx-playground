package com.mghhrn;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.http.HttpServer;
import io.vertx.rxjava.ext.sql.SQLClientHelper;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.handler.BodyHandler;
import io.vertx.rxjava.ext.web.handler.LoggerHandler;
import io.vertx.rxjava.ext.web.handler.ResponseContentTypeHandler;
import io.vertx.rxjava.ext.web.handler.ResponseTimeHandler;
import io.vertx.rxjava.pgclient.PgPool;
import io.vertx.rxjava.sqlclient.*;
import io.vertx.sqlclient.PoolOptions;
import rx.Single;


public class MyFirstRXVerticleV2 extends AbstractVerticle {

    private PgPool pgPool;

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

        pgPool = PgPool.pool(vertx, connectOptions, poolOptions);

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
        serverSingle.subscribe(successfullyCreatedServer -> System.out.println("Http Server created successfully!"),
                error -> {
                    error.printStackTrace();
                    System.out.println("Failed to start HttpServer!");
                } );
    }


    /*************** PRIVATE HANDLERS ***************/

    // rx version for RoutingContext
    private void addProduct(RoutingContext rc) {
        JsonObject body = rc.getBodyAsJson();
        Product product = new Product();
        product.setName(body.getString("name"));
        product.setPrice(body.getLong("price"));

        Single<SqlConnection> connectionSingle = pgPool.rxGetConnection()
                .doOnError(t -> rc.response().setStatusCode(400).end("Failed to retrieve a connection!"));

        connectionSingle.subscribe(connection -> {
            Transaction transaction = connection.begin();
            transaction.rxPreparedQuery("INSERT INTO product (name , price) VALUES ($1 , $2) ",
                    Tuple.of(product.getName(), product.getPrice()))
                    .subscribe(result -> {
                        transaction.rxCommit()
                                .subscribe(onSuccess -> {
                                    connection.close();
                                    System.out.println("Inserted value to DB.  -- row count = " + result.rowCount());
                                    rc.response().end("Got " + result.size() + " rows ");
                                }, error -> {
                                    connection.close();
                                    rc.response().setStatusCode(400).end("Failed to commit transaction. Error = " + error.getMessage());
                                });
                    }, error -> {
                        connection.close();
                        rc.response().setStatusCode(400).end("Failed to run delete query. Error = " + error.getMessage());
                    });
        });
    }


    private void updateProduct(RoutingContext rc) {
        JsonObject jsonBody = rc.getBodyAsJson();
        Long givenId = Long.valueOf(rc.pathParam("id"));
        JsonObject productInDb = null;

        Single<SqlConnection> connectionSingle = pgPool.rxGetConnection()
                .doOnError(t -> rc.response().setStatusCode(400).end("Failed to retrieve a connection!"));

        connectionSingle.subscribe(connection -> {
            Transaction transaction = connection.begin();
            transaction.rxPreparedQuery("select * from product where id = $1",
                    Tuple.of(givenId))
                    .subscribe(result -> {
                        if (result.size() == 0) {
                            transaction.rxRollback()
                                    .subscribe(
                                            onSuccess -> {
                                                connection.close();
                                                rc.response().setStatusCode(400).end("product not found!");
                                            },
                                            error -> {
                                                connection.close();
                                                rc.response().setStatusCode(400).end("error in rollback happened!");
                                            }
                                    );
                        }
                        transaction.rxPreparedQuery("update product set name = $1 , price = $2 where id = $3",
                                Tuple.of(jsonBody.getString("name"), jsonBody.getLong("price"), givenId))
                                .subscribe(onSuccess -> {
                                    transaction.rxCommit()
                                            .subscribe(commitSuccess -> {
                                                connection.close();
                                                rc.response().end("update completed!\n");
                                            }, error -> {
                                                connection.close();
                                                rc.response().setStatusCode(400).end("error committing transaction!");
                                            });
                                }, error -> {
                                    transaction.rxRollback()
                                            .subscribe(rollbackSuccess -> {
                                               connection.close();
                                                rc.response().setStatusCode(400).end("error executing update query!");
                                            });
                                });
                    }, error -> {
                        transaction.rxRollback()
                                .subscribe(onSuccess -> {
                                    connection.close();
                                    rc.response().setStatusCode(400).end("error in executing select query!");
                                });
                    });
        });
    }


    private void listProducts(RoutingContext rc) {

        Single<SqlConnection> connectionSingle = pgPool.rxGetConnection()
                .doOnError(t -> rc.response().setStatusCode(400).end("Failed to retrieve a connection!"));

        connectionSingle.subscribe(connection -> {
            Transaction transaction = connection.begin();
            transaction.rxQuery("SELECT * FROM product")
                    .subscribe(result -> {
                        JsonArray array = new JsonArray();
                        for(Row row : result) {
                            array.add(new JsonObject()
                                    .put("id", row.getLong("id"))
                                    .put("name", row.getString("name"))
                                    .put("price", row.getLong("price"))
                            );
                        }
                        transaction.rxCommit()
                                .subscribe(onSuccess -> {
                                    connection.close();
                                    rc.response().end(array.encodePrettily());
                                }, error -> transaction.rxRollback()
                                                .subscribe(onSuccess -> {
                                                    error.printStackTrace();
                                                    connection.close();
                                                    rc.response()
                                                            .setStatusCode(400)
                                                            .end("Fail to commit the transaction!");
                                                }));
                    }, error -> {
                        System.out.println("Failed to run select query. Cause: " + error.getMessage());
                        error.printStackTrace();
                        transaction.rxRollback()
                                .subscribe(onSuccess -> {
                                    connection.close();
                                    rc.response().setStatusCode(400).end("Failure: " + error.getMessage());
                                });
                    });
        });
    }

    private void getOneProduct(RoutingContext rc) {
        Long productId = Long.valueOf(rc.pathParam("id"));
        Single<SqlConnection> connectionSingle = pgPool.rxGetConnection()
                .doOnError(t -> rc.response().setStatusCode(400).end("Failed to retrieve a connection!"));

        connectionSingle.subscribe(connection -> {
            Transaction transaction = connection.begin();
            Single<RowSet<Row>> selectQuerySingle = transaction.rxPreparedQuery("select name, price from product where id = $1",
                    Tuple.of(productId));
            selectQuerySingle.subscribe(result -> {
                Row row = result.iterator().next();
                transaction.rxCommit()
                        .subscribe(onSuccess -> {
                                    connection.close();
                                    rc.response()
                                            .end(new JsonObject()
                                                    .put("id", productId)
                                                    .put("name", row.getString("name"))
                                                    .put("price", row.getLong("price"))
                                                    .encode());
                                }, error ->
                                    transaction.rxRollback()
                                            .subscribe(onSuccess -> {
                                                error.printStackTrace();
                                                connection.close();
                                                rc.response().setStatusCode(400).end("Fail to commit the transaction!");
                                            })
                                );
            }, error ->
                transaction.rxRollback()
                        .subscribe(onSuccess -> {
                            error.printStackTrace();
                            connection.close();
                            rc.response().setStatusCode(400).end("Fail to run the select query!");
                        })
            );
        });
    }


    private void deleteProduct(RoutingContext rc) {

        Long productId = Long.valueOf(rc.pathParam("id"));
        Single<SqlConnection> connectionSingle = pgPool.rxGetConnection()
                .doOnError(t -> rc.response().setStatusCode(400).end("Failed to retrieve a connection!"));

        connectionSingle.subscribe(connetion -> {
            Transaction transaction = connetion.begin();
            Single<RowSet<Row>> selectRowSetSingle = transaction.rxPreparedQuery("select * from product where id = $1",
                    Tuple.of(productId));

            selectRowSetSingle.subscribe(selectResult -> {
                if (selectResult.size() == 0) {    //check the existence of the data logically
                    transaction.rxRollback()   // Rollback
                            .subscribe(onSuccess -> {
                                connetion.close();
                                rc.response()
                                        .setStatusCode(400)
                                        .end("product with id " + productId + " was not found!");
                            });
                }

                Single<RowSet<Row>> deleteRowSetSingle = transaction.rxPreparedQuery("delete from product where id = $1",
                        Tuple.of(productId));
                deleteRowSetSingle.subscribe(
                        success -> {
                            transaction.rxCommit()
                                    .subscribe(onSuccess -> {
                                        connetion.close();
                                        rc.response().end("delete completed!\n");
                                    });
                        },
                        error -> {
                            transaction.rxRollback()
                                    .subscribe(onSuccess -> {
                                        connetion.close();
                                        rc.response()
                                                .setStatusCode(400)
                                                .end("error executing delete query!");
                                    });
                        }
                );

            }, error ->
                transaction.rxRollback()
                        .subscribe(onSuccess -> {
                            connetion.close();
                            rc.response().setStatusCode(400).end("error in executing select query!");
                        })
            );
        });

    }
}