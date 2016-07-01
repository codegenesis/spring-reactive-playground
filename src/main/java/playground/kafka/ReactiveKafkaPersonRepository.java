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

package playground.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.reactivestreams.Publisher;
import playground.Person;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.FluxConfig;
import reactor.kafka.KafkaFlux;
import reactor.kafka.KafkaSender;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Profile("kafka")
@Repository
public class ReactiveKafkaPersonRepository implements ReactiveCrudRepository<Person, String> {

	private final FluxConfig<String, Person> kafkaFluxConfig;
    private final KafkaSender<String, Person> kafkaSender;
    private final String topic;
    private final Collection<TopicPartition> topicPartitions = new ArrayList<>();
    private final Duration receiveTimeout;

	@Autowired
	public ReactiveKafkaPersonRepository(FluxConfig<String, Person> kafkaFluxConfig, KafkaSender<String, Person> kafkaSender,
	        String topic, long receiveTimeoutMillis) {
		this.kafkaFluxConfig = kafkaFluxConfig;
		this.kafkaSender = kafkaSender;
		this.topic = topic;
		int partitions = kafkaSender.partitionsFor(topic).block().size();
        for (int i = 0; i < partitions; i++)
            topicPartitions.add(new TopicPartition(topic, i));
		this.receiveTimeout = Duration.ofMillis(receiveTimeoutMillis);
	}


	@Override
    public <S extends Person> Mono<S> save(S entity) {
         return kafkaSender.send(new ProducerRecord<String, Person>(topic, entity.getId(), entity))
                           .map(r -> entity);
    }


    @Override
    public <S extends Person> Flux<S> save(Iterable<S> entities) {
        return Flux.fromIterable(entities)
                   .concatMap(p -> kafkaSender.send(new ProducerRecord<String, Person>(topic, p.getId(), p))
                                              .map(r -> p));
    }


    @Override
    public <S extends Person> Flux<S> save(Publisher<S> entityStream) {
        return Flux.from(entityStream)
                .concatMap(p -> kafkaSender.send(new ProducerRecord<String, Person>(topic, p.getId(), p))
                                           .map(r -> p));
    }


    @Override
    public Mono<Person> findOne(String id) {
        return findAll().filter(p -> p.getId().equals(id)).next();
    }


    @Override
    public Mono<Person> findOne(Mono<String> id) {
        return id.then(i -> findOne(i));
    }


    @Override
    public Mono<Boolean> exists(String id) {
        return findAll().filter(p -> p.getId().equals(id)).hasElements();
    }


    @Override
    public Mono<Boolean> exists(Mono<String> id) {
        return id.then(i -> exists(i));
    }

    @Override
    public Flux<Person> findAll() {
        KafkaFlux<String, Person> flux = KafkaFlux.assign(kafkaFluxConfig, topicPartitions);
        return flux.doOnPartitionsAssigned(partitions -> partitions.forEach(p -> p.seekToBeginning()))
                   .filter(record -> record.consumerRecord().value() != null)
                   .map(record -> record.consumerRecord().value())
                   .take(receiveTimeout);
    }


    @Override
    public Flux<Person> findAll(Iterable<String> ids) {
        Set<String> filterIds = new HashSet<>();
        Iterator<String> it = ids.iterator();
        while (it.hasNext())
            filterIds.add(it.next());
        return findAll().filter(person -> filterIds.contains(person.getId()));
    }


    @Override
    public Flux<Person> findAll(Publisher<String> idStream) {
        return findAll(Flux.from(idStream).toIterable());
    }


    @Override
    public Mono<Long> count() {
        return findAll().count();
    }


    @Override
    public Mono<Void> delete(String id) {
        return kafkaSender.send(new ProducerRecord<String, Person>(topic, id, null))
                          .then();
    }


    @Override
    public Mono<Void> delete(Person entity) {
        return delete(entity.getId());
    }


    @Override
    public Mono<Void> delete(Iterable<? extends Person> entities) {
        return Flux.fromIterable(entities)
                   .flatMap(person -> delete(person.getId()))
                   .then();
    }


    @Override
    public Mono<Void> delete(Publisher<? extends Person> entityStream) {
        return Flux.from(entityStream)
                   .flatMap(person -> delete(person.getId()))
                   .then();
    }


    @Override
    public Mono<Void> deleteAll() {
        return findAll().flatMap(person -> delete(person.getId()))
                        .then();
    }

}
