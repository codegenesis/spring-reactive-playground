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

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PersonSerde implements Serializer<Person>, Deserializer<Person> {

    private ObjectMapper objectMapper;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, Person data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public Person deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Person.class);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {
    }


}
