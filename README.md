Spring Reactive Playground is a sandbox for experimenting on applications based on
[Spring Reactive][] Web support, Spring Data Reactive support and async NoSQL drivers.

## Building spring-reactive-playground

### Build reactor-kafka:
 - git clone https://github.com/reactor/reactor-kafka
 - cd reactor-kafka
 - gradle install

### Build spring-reactive-playground:
 - cd spring-reactive-playground
 - gradle jar


## Usage

Server-Sent Events support should work out of the box, just run `Application#main()` and open `http://localhost:8080/` in your browser.

For testing MongoDB support, install MongoDB, run `mongod`, enable `mongo` profile in `application.properties`, and run `Application#main()`:

 - Create a single person: ```curl -i -X POST -H "Content-Type:application/json" -H "Accept: application/json" -d '{"id":"1","firstname":"foo1","lastname":"bar1"}' http://localhost:8080/mongo```
 - Create multiple persons: ```curl -i -X POST -H "Content-Type:application/json" -H "Accept: application/json" -d '[{"id":"2","firstname":"foo2","lastname":"bar2"},{"id":"3","firstname":"foo3","lastname":"bar3"}]' http://localhost:8080/mongo```
 - List all the persons: ```curl -i -H "Accept: application/json" http://localhost:8080/mongo```
 - Get one person: ```curl -i -H "Accept: application/json" http://localhost:8080/mongo/1```

For testing Kafka support, install Kafka, start Zookeeper and Kafk, enable `kafka` profile in `application.properties`, and run `Application#main()`:

 - Create a single person: ```curl -i -X POST -H "Content-Type:application/json" -H "Accept: application/json" -d '{"id":"1","firstname":"foo1","lastname":"bar1"}' http://localhost:8080/kafka```
 - Create multiple persons: ```curl -i -X POST -H "Content-Type:application/json" -H "Accept: application/json" -d '[{"id":"2","firstname":"foo2","lastname":"bar2"},{"id":"3","firstname":"foo3","lastname":"bar3"}]' http://localhost:8080/kafka```
 - List all the persons: ```curl -i -H "Accept: application/json" http://localhost:8080/kafka```
 - Get one person: ```curl -i -H "Accept: application/json" http://localhost:8080/kafka/1```


## License
Spring Reactive Playground is released under version 2.0 of the [Apache License][].

[Apache License]: http://www.apache.org/licenses/LICENSE-2.0
[Spring Reactive]: https://github.com/spring-projects/spring-reactive
