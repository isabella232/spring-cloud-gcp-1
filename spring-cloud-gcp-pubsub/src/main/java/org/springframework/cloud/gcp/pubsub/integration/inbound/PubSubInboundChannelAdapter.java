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

package org.springframework.cloud.gcp.pubsub.integration.inbound;

import java.util.function.Function;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import org.springframework.cloud.gcp.pubsub.core.PubSubOperations;
import org.springframework.cloud.gcp.pubsub.integration.AckMode;
import org.springframework.cloud.gcp.pubsub.support.GcpHeaders;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.util.Assert;

/**
 * Converts from GCP Pub/Sub message to Spring message and sends the Spring message to the
 * attached channels.
 *
 * @author João André Martins
 */
public class PubSubInboundChannelAdapter extends MessageProducerSupport {

	private final String subscriptionName;

	private final PubSubOperations pubSubTemplate;

	private Subscriber subscriber;

	private AckMode ackMode = AckMode.AUTO;

	private Function<ByteString, ?> payloadExtractor;

	public PubSubInboundChannelAdapter(PubSubOperations pubSubTemplate, String subscriptionName) {
		this.pubSubTemplate = pubSubTemplate;
		this.subscriptionName = subscriptionName;
		this.payloadExtractor = ByteString::toStringUtf8;
	}

	@Override
	protected void doStart() {
		super.doStart();

		this.subscriber = this.pubSubTemplate.subscribe(this.subscriptionName, this::receiveMessage);
	}

	private void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		try {
			AbstractIntegrationMessageBuilder<?> messageBuilder = getMessageBuilderFactory()
					.withPayload(this.payloadExtractor.apply(message.getData()));
			messageBuilder.copyHeaders(message.getAttributesMap());
			if (this.ackMode == AckMode.MANUAL) {
				// Send the consumer downstream so user decides on when to ack/nack.
				messageBuilder.setHeader(GcpHeaders.ACKNOWLEDGEMENT, consumer);
			}
			sendMessage(messageBuilder.build());
		}
		catch (RuntimeException re) {
			if (this.ackMode == AckMode.AUTO) {
				consumer.nack();
			}
			throw re;
		}

		if (this.ackMode == AckMode.AUTO) {
			consumer.ack();
		}
	}

	@Override
	protected void doStop() {
		if (this.subscriber != null) {
			this.subscriber.stopAsync();
		}

		super.doStop();
	}

	public AckMode getAckMode() {
		return this.ackMode;
	}

	public void setAckMode(AckMode ackMode) {
		Assert.notNull(ackMode, "The acknowledgement mode can't be null.");
		this.ackMode = ackMode;
	}

	public void setPayloadExtractor(Function<ByteString, ?> payloadExtractor) {
		Assert.notNull(payloadExtractor, "The specified payload extractor can't be null.");
		this.payloadExtractor = payloadExtractor;
	}

	public Function<ByteString, ?> getPayloadExtractor() {
		return this.payloadExtractor;
	}
}
