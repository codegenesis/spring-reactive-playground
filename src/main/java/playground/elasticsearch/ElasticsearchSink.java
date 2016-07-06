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
package playground.elasticsearch;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import playground.Person;
import playground.etl.Sink;
import reactor.core.publisher.Flux;
import reactor.kafka.ConsumerOffset;

@Profile({"elasticsearch"})
@Repository
public class ElasticsearchSink implements Sink<String, Person> {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSink.class);

    private final Client client;
    private final String documentName;

    public ElasticsearchSink(InetSocketAddress serverAddress, String clusterName, String documentName) {
        Settings settings = Settings.builder()
                                    .put("cluster.name", clusterName)
                                    .build();
        client = TransportClient.builder()
                                .settings(settings)
                                .build()
                                .addTransportAddress(new InetSocketTransportAddress(serverAddress));
        this.documentName = documentName;
    }

    @Override
    public Flux<Person> put(Flux<? extends Sink.Record<String, Person>> kafkaRecords) {
        BulkRequestBuilder bulkReqBuilder = client.prepareBulk();
        return kafkaRecords
            .map(record -> {
                    ConsumerOffset offset = record.consumerOffset();
                    Person person = record.value();
                    String index = (offset.topicPartition() + "-" + offset.offset()).toLowerCase();
                    IndexRequestBuilder indexBuilder = client.prepareIndex(index, documentName, record.key());
                    Map<String, Object> personMap = new HashMap<>();
                    personMap.put("firstname", person.getFirstname());
                    personMap.put("lastname", person.getLastname());
                    personMap.put("validationDate", person.getValidationDate());
                    bulkReqBuilder.add(indexBuilder.setSource(personMap));
                    return person;
                })
            .collectList()
            .flatMap(list -> {
                    return Flux.create(emitter -> bulkReqBuilder.execute(new ActionListener<BulkResponse>() {

                        @Override
                        public void onResponse(BulkResponse response) {
                            for (Person person : list)
                                emitter.next(person);
                            emitter.complete();
                            logger.debug("Bulk request to store in Elasticsearch succeeded: {}", list);
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.error("Bulk request to store in Elasticsearch failed", e);
                            emitter.fail(e);
                        }
                    }));
            });
    }
}
