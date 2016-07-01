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

import playground.Person;
import reactor.kafka.FluxConfig;
import reactor.kafka.KafkaSender;
import reactor.kafka.SenderConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("kafka")
@Configuration
public class KafkaConfiguration {

	@Value("${kafka.bootstrap.servers}")
	private String bootstrapServers;

    @Value("${kafka.group.id}")
    private String groupId;

    @Value("${kafka.topic}")
    private String topic;

    @Value("${kafka.receive.timeout.millis}")
    private long receiveTimeoutMillis;

	@Bean
	FluxConfig<String, Person> fluxConfig() {
	    Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PersonSerde.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

	    return new FluxConfig<String, Person>(props).pollTimeout(Duration.ofMillis(1000));
	}

	@Bean
    SenderConfig<String, Person> senderConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PersonSerde.class.getName());

        return new SenderConfig<>(props);
    }

	@Bean
    KafkaSender<String, Person> kafkaSender() {
        return new KafkaSender<>(senderConfig());
    }

	@Bean
    String groupId() {
	    return groupId;
    }

    @Bean
    String topic() {
        return topic;
    }

    @Bean
    long receiveTimeoutMillis() {
        return receiveTimeoutMillis;
    }
}
