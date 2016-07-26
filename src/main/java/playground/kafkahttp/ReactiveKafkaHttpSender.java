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

package playground.kafkahttp;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.kafka.KafkaSender;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

@Profile("kafkahttp")
@Repository
public class ReactiveKafkaHttpSender {

    private final KafkaSender<String, byte[]> kafkaSender;

	public ReactiveKafkaHttpSender(KafkaSender<String, byte[]> kafkaSender) {
		this.kafkaSender = kafkaSender;
	}

    public Flux<RecordMetadata> sendToKafka(String topic, Publisher<Records> recordStream) {
        return Flux.from(recordStream)
                   .concatMap(records -> Flux.fromArray(records.getRecords()))
                   .concatMap(r -> kafkaSender.send(new ProducerRecord<>(topic, r.getValue())));
    }
}
