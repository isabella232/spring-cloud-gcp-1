/*
 *  Copyright 2017 original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.cloud.gcp.autoconfigure.pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.gcp.autoconfigure.core.GcpContextAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * @author Andreas Berger
 */
@Configuration
public class GcpPubSubEmulatorConfigurationTests {

	private AnnotationConfigApplicationContext context;

	@After
	public void closeContext() {
		if (this.context != null) {
			this.context.close();
		}
	}

	@Test
	public void testEmulatorConfig() {
		// loadEnvironment();
		loadEnvironment("PUBSUB_EMULATOR_HOST=localhost:8085");
		CredentialsProvider credentialsProvider = this.context.getBean(CredentialsProvider.class);
		Assert.assertTrue("CredentialsProvider is not correct",
				credentialsProvider instanceof NoCredentialsProvider);

		TransportChannelProvider transportChannelProvider = this.context.getBean(TransportChannelProvider.class);
		Assert.assertTrue("TransportChannelProvider is not correct",
				transportChannelProvider instanceof FixedTransportChannelProvider);
	}

	private void loadEnvironment(String... environment) {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of(environment).applyTo(context);
		context.register(
				GcpPubSubEmulatorConfiguration.class,
				GcpContextAutoConfiguration.class,
				GcpPubSubAutoConfiguration.class);
		context.register(this.getClass());
		context.refresh();
		this.context = context;
	}

}
