#Vertx Playgroud

#Create vertx maven project
mvn io.fabric8:vertx-maven-plugin:1.0.5:setup -DprojectGroupId=io.vertx.microservice -DprojectArtifactId=hello-microservice-message -Dverticle=io.vertx.book.message.HelloMicroservice -Ddependencies=infinispan

#Run vertx
mvn compile vertx:run
mvn compile vertx:run -Dvertx.runArgs="-cluster -Djava.net.preferIPv4Stack=true"

mvn clean package
java -jar target/file.jar 
java -jar target/file.jar --cluster -Djava.net.preferIPv4Stack=true




#Commands to send a request

curl -v -i --header "Content-Type: application/json"  localhost:8080

curl -v -i --request POST --header "Content-Type: application/json" --data '{"name":"Phone","price":2400}' localhost:8080/product


