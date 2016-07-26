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

import reactor.core.publisher.Flux;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Profile("kafkahttp")
@RestController
public class KafkaHttpController {

	private final ReactiveKafkaHttpSender sender;

	public KafkaHttpController(ReactiveKafkaHttpSender sender) {
		this.sender = sender;
	}

	@RequestMapping(path = "/kafkahttp/{topic}", method = RequestMethod.POST)
	public Flux<SendResponse> sendToKafka(@PathVariable String topic, @RequestBody Flux<Records> binaryStream) {
		return this.sender.sendToKafka(topic, binaryStream)
		           .map(metadata -> new SendResponse(metadata));
	}
}
