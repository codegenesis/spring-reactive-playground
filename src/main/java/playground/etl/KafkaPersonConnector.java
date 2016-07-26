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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import playground.Person;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.ConsumerMessage;
import reactor.kafka.FluxConfig;
import reactor.kafka.KafkaFlux;
import reactor.kafka.KafkaSender;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

@Profile("etl")
@Repository
public class KafkaPersonConnector {

    private final FluxConfig<String, Person> fluxConfig;
    private final KafkaSender<String, Person> sender;
    private final String topic;
    private final Collection<TopicPartition> topicPartitions = new ArrayList<>();
    private final Duration receiveTimeout;

    @Autowired
    public KafkaPersonConnector(FluxConfig<String, Person> kafkaFluxConfig, KafkaSender<String, Person> kafkaSender,
            String topic, Duration receiveTimeout) {
        this.fluxConfig = kafkaFluxConfig;
        this.sender = kafkaSender;
        this.topic = topic;
        int partitions = kafkaSender.partitionsFor(topic).block().size();
        for (int i = 0; i < partitions; i++)
            topicPartitions.add(new TopicPartition(topic, i));
        this.receiveTimeout = receiveTimeout;
    }


    public Mono<Person> store(Person person) {
        return sender.send(new ProducerRecord<String, Person>(topic, person.getId(), person))
                     .map(recordMetadata -> person);
    }

    public Flux<ConsumerMessage<String, Person>> createFlux() {
        return KafkaFlux.<String, Person>listenOn(fluxConfig, Collections.singleton(topic))
                        .take(receiveTimeout);
    }

}
