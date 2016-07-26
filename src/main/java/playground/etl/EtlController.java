/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package playground.etl;

import playground.Person;
import playground.elasticsearch.ElasticsearchSink;
import playground.mongo.ReactiveMongoPersonRepository;
import reactor.core.publisher.Flux;
import reactor.kafka.ConsumerMessage;
import reactor.kafka.ConsumerOffset;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Profile({"etl"})
@RestController
public class EtlController {
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("dd-MM-yy");

    private final ReactiveCrudRepository<Person, String> source;
    private final KafkaPersonConnector kafkaConnector;
    private final Sink<String, Person> elasticsearchSink;
    private final Duration sinkBufferTimeout = Duration.ofMillis(1000);
    private final int sinkBufferSize = 1000;

    @Autowired
    public EtlController(ReactiveMongoPersonRepository mongoSource,
            KafkaPersonConnector kafkaConnector,
            ElasticsearchSink elasticsearchSink) {
        this.source = mongoSource;
        this.kafkaConnector = kafkaConnector;
        this.elasticsearchSink = elasticsearchSink;
    }

    @RequestMapping(path = "/etl/elasticsearch", method = RequestMethod.POST)
    public Flux<Person> etlElasticsearch() {
        return etl(elasticsearchSink);
    }
    private Flux<Person> etl(Sink<String, Person> sink) {

        return kafkaConnector.createFlux()
                             .doOnSubscribe(s ->
                                    source.findAll()                                                  // Extract from MongoDB source
                                          .flatMap(person -> kafkaConnector.store(person))            // store in Kafka
                                          .subscribe()
                                  )
                             .map(kafkaMessage -> new PersonRecord(kafkaMessage).validate())          // Transform messages consumed from Kafka
                             .window(sinkBufferSize, sinkBufferTimeout)                               // batch outgoing
                             .flatMap(recordList -> sink.put(recordList));                            // Load into sink warehouse
    }

    private static class PersonRecord implements Sink.Record<String, Person> {
        final Person person;
        final ConsumerOffset consumerOffset;
        PersonRecord(ConsumerMessage<String, Person> kafkaMessage) {
            this.person = kafkaMessage.consumerRecord().value();
            this.consumerOffset = kafkaMessage.consumerOffset();
        }

        PersonRecord validate() {
            if (person.getValidationDate() == null)
                person.setValidationDate(dateFormatter.format(new Date()));
            return this;
        }

        @Override
        public String key() {
            return person.getId() == null ? person.getFirstname() + "-" + person.getLastname() : person.getId();
        }

        @Override
        public Person value() {
            return person;
        }

        @Override
        public ConsumerOffset consumerOffset() {
            return consumerOffset;
        }
    }
}
