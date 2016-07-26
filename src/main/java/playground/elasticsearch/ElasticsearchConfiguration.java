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
import java.util.Collections;

import org.apache.kafka.clients.ClientUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("elasticsearch")
@Configuration
public class ElasticsearchConfiguration {

    @Value("${elasticsearch.server}")
    private String serverAddress;

    @Value("${elasticsearch.cluster}")
    private String clusterName;

    @Value("${elasticsearch.document}")
    private String documentName;

    @Bean
    public InetSocketAddress serverAddress() {
        return ClientUtils.parseAndValidateAddresses(Collections.singletonList(serverAddress)).get(0);
    }

    @Bean
    public String clusterName() {
        return clusterName;
    }

    @Bean
    public String documentName() {
        return documentName;
    }

}