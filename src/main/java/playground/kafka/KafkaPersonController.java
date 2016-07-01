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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Profile("kafka")
@RestController
public class KafkaPersonController {

	private final ReactiveKafkaPersonRepository repository;

	@Autowired
	public KafkaPersonController(ReactiveKafkaPersonRepository repository) {
		this.repository = repository;
	}

	@RequestMapping(path = "/kafka", method = RequestMethod.POST)
	public Mono<Void> create(@RequestBody Flux<Person> personStream) {
		return this.repository.save(personStream).then();
	}

	@RequestMapping(path = "/kafka", method = RequestMethod.GET)
	public Flux<Person> list() {
		return this.repository.findAll();
	}

	@RequestMapping(path = "/kafka/{id}", method = RequestMethod.GET)
    public Mono<Person> findById(@PathVariable String id) {
		return this.repository.findOne(id);
	}

}
