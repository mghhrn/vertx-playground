# Vertx Playgroud

## Create vertx maven project
* mvn io.fabric8:vertx-maven-plugin:1.0.5:setup -DprojectGroupId=io.vertx.microservice -DprojectArtifactId=hello-microservice-message -Dverticle=io.vertx.book.message.HelloMicroservice -Ddependencies=infinispan

## Run vertx
* mvn compile vertx:run
* mvn compile vertx:run -Dvertx.runArgs="-cluster -Djava.net.preferIPv4Stack=true"

------------

https://vertx.io/docs/vertx-core/java/#_run_verticles
* mvn clean package

If you run application directly without "start" command, you must pass JVM args before "jar" command
* java -Xmx200m -Xms20m -jar target/vertx-playground-1.0.jar --cluster --conf verticle_context.conf -Djava.net.preferIPv4Stack=true

If you use "start" command you must pass JVM args as "java-opts" argument (not working?!):
* java -jar target/vertx-playground-1.0.jar start --cluster -Djava.net.preferIPv4Stack=true --conf verticle_context.conf --vertx-id=my-app-name --java-opts="-Xmx300m -Xms10m"

#### Development run with atuo redeploy:
* mvn clean compile vertx:run -Dvertx.runArgs="--cluster -Djava.net.preferIPv4Stack=true --conf verticle_context.conf" -Dvertx.jvmArguments="-Xmx500m -Xms100m"





## Commands to send a request

* curl -v -i --header "Content-Type: application/json"  localhost:8080

* curl -v -i --request POST --header "Content-Type: application/json" --data '{"name":"Phone","price":2400}' localhost:8080/product




