/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.integration.kafka;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.Lifecycle;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.integration.kafka.core.ConnectionFactory;
import org.springframework.integration.kafka.core.DefaultConnectionFactory;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.core.ZookeeperConfiguration;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.integration.kafka.listener.KafkaTopicOffsetManager;
import org.springframework.integration.kafka.listener.OffsetManager;
import org.springframework.integration.kafka.support.ProducerConfiguration;
import org.springframework.integration.kafka.support.ProducerFactoryBean;
import org.springframework.integration.kafka.support.ProducerMetadata;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.integration.x.kafka.WindowingOffsetManager;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;
import org.springframework.xd.dirt.integration.bus.AbstractBusPropertiesAccessor;
import org.springframework.xd.dirt.integration.bus.Binding;
import org.springframework.xd.dirt.integration.bus.BusProperties;
import org.springframework.xd.dirt.integration.bus.EmbeddedHeadersMessageConverter;
import org.springframework.xd.dirt.integration.bus.MessageBusSupport;
import org.springframework.xd.dirt.integration.bus.XdHeaders;
import org.springframework.xd.dirt.integration.bus.serializer.MultiTypeCodec;

import kafka.admin.AdminUtils;
import kafka.api.OffsetRequest;
import kafka.api.TopicMetadata;
import kafka.common.ErrorMapping;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.producer.Producer;
import kafka.producer.DefaultPartitioner;
import kafka.producer.KeyedMessage;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.DefaultEncoder;
import kafka.serializer.StringEncoder;
import kafka.utils.ZkUtils;
import scala.collection.Seq;

/**
 * A message bus that uses Kafka as the underlying middleware.
 * The general implementation mapping between XD concepts and Kafka concepts is as follows:
 * <table>
 * <tr>
 * <th>Stream definition</th><th>Kafka topic</th><th>Kafka partitions</th><th>Notes</th>
 * </tr>
 * <tr>
 * <td>foo = "http | log"</td><td>foo.0</td><td>1 partition</td><td>1 producer, 1 consumer</td>
 * </tr>
 * <tr>
 * <td>foo = "http | log", log.count=x</td><td>foo.0</td><td>x partitions</td><td>1 producer, x consumers with static group 'springXD', achieves queue semantics</td>
 * </tr>
 * <tr>
 * <td>foo = "http | log", log.count=x + XD partitioning</td><td>still 1 topic 'foo.0'</td><td>x partitions + use key computed by XD</td><td>1 producer, x consumers with static group 'springXD', achieves queue semantics</td>
 * </tr>
 * <tr>
 * <td>foo = "http | log", log.count=x, concurrency=y</td><td>foo.0</td><td>x*y partitions</td><td>1 producer, x XD consumers, each with y threads</td>
 * </tr>
 * <tr>
 * <td>foo = "http | log", log.count=0, x actual log containers</td><td>foo.0</td><td>10(configurable) partitions</td><td>1 producer, x XD consumers. Can't know the number of partitions beforehand, so decide a number that better be greater than number of containers</td>
 * </tr>
 * </table>
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 */
public class KafkaMessageBus extends MessageBusSupport {

	private final AtomicInteger correlationIdCounter = new AtomicInteger(new Random().nextInt());

	public static final int METADATA_VERIFICATION_RETRY_ATTEMPTS = 10;

	public static final double METADATA_VERIFICATION_RETRY_BACKOFF_MULTIPLIER = 2;

	public static final int METADATA_VERIFICATION_RETRY_INITIAL_INTERVAL = 100;

	public static final int METADATA_VERIFICATION_MAX_INTERVAL = 1000;

	public static final String BATCHING_ENABLED = "batchingEnabled";

	public static final String BATCH_SIZE = "batchSize";

	public static final String BATCH_TIMEOUT = "batchTimeout";

	public static final String REPLICATION_FACTOR = "replicationFactor";

	public static final String CONCURRENCY = "concurrency";

	public static final String REQUIRED_ACKS = "requiredAcks";

	public static final String COMPRESSION_CODEC = "compressionCodec";

	private static final String DEFAULT_COMPRESSION_CODEC = "default";

	private static final int DEFAULT_REQUIRED_ACKS = 1;

	private RetryOperations retryOperations;

	/**
	 * Used when writing directly to ZK. This is what Kafka expects.
	 */
	public final static ZkSerializer utf8Serializer = new ZkSerializer() {

		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			try {
				return ((String) data).getBytes("UTF-8");
			}
			catch (UnsupportedEncodingException e) {
				throw new ZkMarshallingError(e);
			}
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			try {
				return new String(bytes, "UTF-8");
			}
			catch (UnsupportedEncodingException e) {
				throw new ZkMarshallingError(e);
			}
		}
	};

	protected static final Set<Object> PRODUCER_COMPRESSION_PROPERTIES = new HashSet<Object>(
			Arrays.asList(new String[] {
					KafkaMessageBus.COMPRESSION_CODEC,
			}));

	/**
	 * The consumer group to use when achieving point to point semantics (that
	 * consumer group name is static and hence shared by all containers).
	 */
	private static final String POINT_TO_POINT_SEMANTICS_CONSUMER_GROUP = "springXD";

	/**
	 * Basic + concurrency + partitioning.
	 */
	private static final Set<Object> SUPPORTED_CONSUMER_PROPERTIES = new SetBuilder()
			.addAll(CONSUMER_STANDARD_PROPERTIES)
			.add(BusProperties.PARTITION_INDEX) // Not actually used
			.add(BusProperties.COUNT) // Not actually used
			.add(BusProperties.CONCURRENCY)
			.build();

	/**
	 * Basic + concurrency.
	 */
	private static final Set<Object> SUPPORTED_NAMED_CONSUMER_PROPERTIES = new SetBuilder()
			.addAll(CONSUMER_STANDARD_PROPERTIES)
			.build();

	private static final Set<Object> SUPPORTED_NAMED_PRODUCER_PROPERTIES = new SetBuilder()
			.addAll(PRODUCER_STANDARD_PROPERTIES)
			.addAll(PRODUCER_BATCHING_BASIC_PROPERTIES)
			.build();

	/**
	 * Partitioning + kafka producer properties.
	 */
	private static final Set<Object> SUPPORTED_PRODUCER_PROPERTIES = new SetBuilder()
			.addAll(PRODUCER_PARTITIONING_PROPERTIES)
			.addAll(PRODUCER_STANDARD_PROPERTIES)
			.add(BusProperties.DIRECT_BINDING_ALLOWED)
			.addAll(PRODUCER_BATCHING_BASIC_PROPERTIES)
			.addAll(PRODUCER_COMPRESSION_PROPERTIES)
			.build();

	private final EmbeddedHeadersMessageConverter embeddedHeadersMessageConverter = new EmbeddedHeadersMessageConverter();

	private final ZookeeperConnect zookeeperConnect;

	private String brokers;

	private ExecutorService executor = Executors.newCachedThreadPool();

	private String[] headersToMap;

	private String zkAddress;

	// -------- Default values for properties -------
	private int defaultReplicationFactor = 1;

	private String defaultCompressionCodec = DEFAULT_COMPRESSION_CODEC;

	private int defaultRequiredAcks = DEFAULT_REQUIRED_ACKS;

	private int defaultQueueSize = 1000;

	private ConnectionFactory connectionFactory;

	private String offsetStoreTopic = "SpringXdOffsets";

	private int socketBufferSize = 2097152;

	private int offsetStoreSegmentSize = 250 * 1024 * 1024;

	private int offsetStoreRetentionTime = 60000;

	private int offsetStoreRequiredAcks = 1;

	private int offsetStoreMaxFetchSize = 1048576;

	private boolean offsetStoreBatchEnabled = false;

	private int offsetStoreBatchSize = 200;

	private int offsetStoreBatchTime = 1000;

	private int offsetUpdateTimeWindow = 10000;

	private int offsetUpdateCount = 0;

	private int offsetUpdateShutdownTimeout = 2000;


	public KafkaMessageBus(ZookeeperConnect zookeeperConnect, String brokers, String zkAddress,
			MultiTypeCodec<Object> codec, String... headersToMap) {
		this.zookeeperConnect = zookeeperConnect;
		this.brokers = brokers;
		this.zkAddress = zkAddress;
		setCodec(codec);
		if (headersToMap.length > 0) {
			String[] combinedHeadersToMap =
					Arrays.copyOfRange(XdHeaders.STANDARD_HEADERS, 0, XdHeaders.STANDARD_HEADERS.length + headersToMap
							.length);
			System.arraycopy(headersToMap, 0, combinedHeadersToMap, XdHeaders.STANDARD_HEADERS.length, headersToMap
					.length);
			this.headersToMap = combinedHeadersToMap;
		}
		else {
			this.headersToMap = XdHeaders.STANDARD_HEADERS;
		}

	}

	public void setOffsetStoreTopic(String offsetStoreTopic) {
		this.offsetStoreTopic = offsetStoreTopic;
	}

	public void setOffsetStoreSegmentSize(int offsetStoreSegmentSize) {
		this.offsetStoreSegmentSize = offsetStoreSegmentSize;
	}

	public void setOffsetStoreRetentionTime(int offsetStoreRetentionTime) {
		this.offsetStoreRetentionTime = offsetStoreRetentionTime;
	}

	public void setSocketBufferSize(int socketBufferSize) {
		this.socketBufferSize = socketBufferSize;
	}

	public void setOffsetStoreRequiredAcks(int offsetStoreRequiredAcks) {
		this.offsetStoreRequiredAcks = offsetStoreRequiredAcks;
	}

	public void setOffsetStoreMaxFetchSize(int offsetStoreMaxFetchSize) {
		this.offsetStoreMaxFetchSize = offsetStoreMaxFetchSize;
	}


	public void setOffsetUpdateTimeWindow(int offsetUpdateTimeWindow) {
		this.offsetUpdateTimeWindow = offsetUpdateTimeWindow;
	}

	public void setOffsetUpdateCount(int offsetUpdateCount) {
		this.offsetUpdateCount = offsetUpdateCount;
	}

	public void setOffsetUpdateShutdownTimeout(int offsetUpdateShutdownTimeout) {
		this.offsetUpdateShutdownTimeout = offsetUpdateShutdownTimeout;
	}

	public void setOffsetStoreBatchEnabled(boolean offsetStoreBatchEnabled) {
		this.offsetStoreBatchEnabled = offsetStoreBatchEnabled;
	}

	public void setOffsetStoreBatchSize(int offsetStoreBatchSize) {
		this.offsetStoreBatchSize = offsetStoreBatchSize;
	}

	public void setOffsetStoreBatchTime(int offsetStoreBatchTime) {
		this.offsetStoreBatchTime = offsetStoreBatchTime;
	}

	/**
	 * Retry configuration for operations such as validating topic creation
	 *
	 * @param retryOperations
	 */
	public void setRetryOperations(RetryOperations retryOperations) {
		this.retryOperations = retryOperations;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// we instantiate the connection factory here due to https://jira.spring.io/browse/XD-2647
		ZookeeperConfiguration configuration = new ZookeeperConfiguration(this.zookeeperConnect);
		configuration.setBufferSize(socketBufferSize);
		DefaultConnectionFactory defaultConnectionFactory =
				new DefaultConnectionFactory(configuration);
		defaultConnectionFactory.afterPropertiesSet();
		this.connectionFactory = defaultConnectionFactory;
		if (retryOperations == null) {
			RetryTemplate retryTemplate = new RetryTemplate();

			SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
			simpleRetryPolicy.setMaxAttempts(METADATA_VERIFICATION_RETRY_ATTEMPTS);
			retryTemplate.setRetryPolicy(simpleRetryPolicy);

			ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
			backOffPolicy.setInitialInterval(METADATA_VERIFICATION_RETRY_INITIAL_INTERVAL);
			backOffPolicy.setMultiplier(METADATA_VERIFICATION_RETRY_BACKOFF_MULTIPLIER);
			backOffPolicy.setMaxInterval(METADATA_VERIFICATION_MAX_INTERVAL);
			retryTemplate.setBackOffPolicy(backOffPolicy);
			retryOperations = retryTemplate;
		}
	}

	/**
	 * Allowed chars are ASCII alphanumerics, '.', '_' and '-'.
	 * '_' is used as escaped char in the form '_xx' where xx is the hexadecimal
	 * value of the byte(s) needed to represent an illegal char in utf8.
	 */
	/*default*/
	public static String escapeTopicName(String original) {
		StringBuilder result = new StringBuilder(original.length());
		try {
			byte[] utf8 = original.getBytes("UTF-8");
			for (byte b : utf8) {
				if ((b >= 'a') && (b <= 'z') || (b >= 'A') && (b <= 'Z') || (b >= '0') && (b <= '9') || (b == '.')
						|| (b == '-')) {
					result.append((char) b);
				}
				else {
					result.append(String.format("_%02X", b));
				}
			}
		}
		catch (UnsupportedEncodingException e) {
			throw new AssertionError(e); // Can't happen
		}
		return result.toString();
	}

	public void setDefaultReplicationFactor(int defaultReplicationFactor) {
		this.defaultReplicationFactor = defaultReplicationFactor;
	}

	public void setDefaultCompressionCodec(String defaultCompressionCodec) {
		this.defaultCompressionCodec = defaultCompressionCodec;
	}

	public void setDefaultRequiredAcks(int defaultRequiredAcks) {
		this.defaultRequiredAcks = defaultRequiredAcks;
	}

	public void setDefaultQueueSize(int defaultQueueSize) {
		this.defaultQueueSize = defaultQueueSize;
	}

	@Override
	public void bindConsumer(String name, final MessageChannel moduleInputChannel, Properties properties) {
		// Point-to-point consumers reset at the earliest time, which allows them to catch up with all messages
		createKafkaConsumer(name, moduleInputChannel, properties, POINT_TO_POINT_SEMANTICS_CONSUMER_GROUP,
				OffsetRequest.EarliestTime());
		bindExistingProducerDirectlyIfPossible(name, moduleInputChannel);
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inputChannel, Properties properties) {
		// Usage of a different consumer group each time achieves pub-sub
		// PubSub consumers reset at the latest time, which allows them to receive only messages sent after
		// they've been bound
		String group = UUID.randomUUID().toString();
		createKafkaConsumer(name, inputChannel, properties, group, OffsetRequest.LatestTime());
	}

	@Override
	public void bindProducer(final String name, MessageChannel moduleOutputChannel, Properties properties) {

		Assert.isInstanceOf(SubscribableChannel.class, moduleOutputChannel);
		KafkaPropertiesAccessor accessor = new KafkaPropertiesAccessor(properties);
		if (name.startsWith(P2P_NAMED_CHANNEL_TYPE_PREFIX)) {
			validateProducerProperties(name, properties, SUPPORTED_NAMED_PRODUCER_PROPERTIES);
		}
		else {
			validateProducerProperties(name, properties, SUPPORTED_PRODUCER_PROPERTIES);
		}
		if (!bindNewProducerDirectlyIfPossible(name, (SubscribableChannel) moduleOutputChannel, accessor)) {
			if (logger.isInfoEnabled()) {
				logger.info("Using kafka topic for outbound: " + name);
			}

			final String topicName = escapeTopicName(name);
			int numPartitions = accessor.getNumberOfKafkaPartitionsForProducer();

			TopicMetadata targetTopicMetadata = ensureTopicCreated(topicName, numPartitions, defaultReplicationFactor);


			ProducerMetadata<String, byte[]> producerMetadata = new ProducerMetadata<String, byte[]>(
					topicName);
			producerMetadata.setValueEncoder(new DefaultEncoder(null));
			producerMetadata.setValueClassType(byte[].class);
			producerMetadata.setKeyEncoder(new StringEncoder(null));
			producerMetadata.setKeyClassType(String.class);
			producerMetadata.setCompressionCodec(accessor.getCompressionCodec(this.defaultCompressionCodec));
			producerMetadata.setPartitioner(new DefaultPartitioner(null));

			Properties additionalProps = new Properties();
			additionalProps.put("request.required.acks", String.valueOf(accessor.getRequiredAcks(this.defaultRequiredAcks)));
			if (accessor.isBatchingEnabled(this.defaultBatchingEnabled)) {
				producerMetadata.setAsync(true);
				producerMetadata.setBatchNumMessages(String.valueOf(accessor.getBatchSize(this.defaultBatchSize)));
				additionalProps.put("queue.buffering.max.ms", String.valueOf(accessor.getBatchTimeout(this.defaultBatchTimeout)));
			}

			ProducerFactoryBean<String, byte[]> producerFB =
					new ProducerFactoryBean<String, byte[]>(producerMetadata, brokers, additionalProps);

			try {
				final Producer<String, byte[]> producer = producerFB.getObject();


				final ProducerConfiguration<String, byte[]> producerConfiguration
						= new ProducerConfiguration<String, byte[]>(producerMetadata, producer);

				MessageHandler messageHandler = new AbstractMessageHandler() {

					@Override
					protected void handleMessageInternal(Message<?> message) throws Exception {
						// strip off the message key used internally by the bus and use a partitioning key for partitioning
						producerConfiguration.getProducer()
								.send(new KeyedMessage<String, byte[]>(topicName, null, message.getHeaders().get("messageKey", Integer.class), (byte[]) message.getPayload()));
					}
				};

				MessageHandler handler = new SendingHandler(messageHandler, topicName, accessor,
						targetTopicMetadata.partitionsMetadata().size());
				EventDrivenConsumer consumer = new EventDrivenConsumer((SubscribableChannel) moduleOutputChannel, handler);
				consumer.setBeanFactory(this.getBeanFactory());
				consumer.setBeanName("outbound." + name);
				consumer.afterPropertiesSet();
				Binding producerBinding = Binding.forProducer(name, moduleOutputChannel, consumer, accessor);
				addBinding(producerBinding);
				producerBinding.start();
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}

		}

	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outputChannel, Properties properties) {
		bindProducer(name, outputChannel, properties);
	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Auto-generated method stub");
	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Auto-generated method stub");
	}

	/**
	 * Creates a Kafka topic if needed, or try to increase its partition count to the desired number.
	 */
	private TopicMetadata ensureTopicCreated(final String topicName, final int numPartitions, int replicationFactor) {

		final int sessionTimeoutMs = 10000;
		final int connectionTimeoutMs = 10000;
		final ZkClient zkClient = new ZkClient(zkAddress, sessionTimeoutMs, connectionTimeoutMs, utf8Serializer);
		try {
			// The following is basically copy/paste from AdminUtils.createTopic() with
			// createOrUpdateTopicPartitionAssignmentPathInZK(..., update=true)
			Properties topicConfig = new Properties();
			Seq<Object> brokerList = ZkUtils.getSortedBrokerList(zkClient);
			scala.collection.Map<Object, Seq<Object>> replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList,
					numPartitions, replicationFactor, -1, -1);
			AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topicName, replicaAssignment, topicConfig,
					true);


			try {
				TopicMetadata topicMetadata = retryOperations.execute(new RetryCallback<TopicMetadata, Exception>() {
					@Override
					public TopicMetadata doWithRetry(RetryContext context) throws Exception {
						TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topicName, zkClient);
						if (topicMetadata.errorCode() != ErrorMapping.NoError() || !topicName.equals(topicMetadata.topic())) {
							// downcast to Exception because that's what the error throws
							throw (Exception) ErrorMapping.exceptionFor(topicMetadata.errorCode());
						}
						List<PartitionMetadata> partitionMetadatas = new kafka.javaapi.TopicMetadata(topicMetadata).partitionsMetadata();
						if (partitionMetadatas.size() != numPartitions) {
							throw new IllegalStateException("The number of expected partitions was: " + numPartitions + ", but " +
									partitionMetadatas.size() + " have been found instead");
						}
						for (PartitionMetadata partitionMetadata : partitionMetadatas) {
							if (partitionMetadata.errorCode() != ErrorMapping.NoError()) {
								throw (Exception) ErrorMapping.exceptionFor(partitionMetadata.errorCode());
							}
						}
						return topicMetadata;
					}
				});
				// work around an issue in Spring
				this.connectionFactory.refreshMetadata(Collections.<String>emptySet());

				return topicMetadata;
			}
			catch (Exception e) {
				logger.error("Cannot initialize MessageBus", e);
				throw new RuntimeException("Cannot initialize message bus:", e);
			}

		}
		finally {
			zkClient.close();
		}
	}

	private void createKafkaConsumer(String name, final MessageChannel moduleInputChannel, Properties properties,
			String group, long referencePoint) {

		if (name.startsWith(P2P_NAMED_CHANNEL_TYPE_PREFIX)) {
			validateConsumerProperties(name, properties, SUPPORTED_NAMED_CONSUMER_PROPERTIES);
		}
		else {
			validateConsumerProperties(name, properties, SUPPORTED_CONSUMER_PROPERTIES);
		}
		KafkaPropertiesAccessor accessor = new KafkaPropertiesAccessor(properties);

		int concurrency = accessor.getConcurrency(defaultConcurrency);
		String topic = escapeTopicName(name);

		ensureTopicCreated(topic, accessor.getCount() * concurrency, defaultReplicationFactor);

		connectionFactory.refreshMetadata(Collections.singleton(topic));

		Decoder<byte[]> valueDecoder = new DefaultDecoder(null);
		Decoder<Integer> keyDecoder = new IntegerEncoderDecoder();

		Collection<Partition> allPartitions = connectionFactory.getPartitions(topic);

		Collection<Partition> listenedPartitions;

		int moduleCount = accessor.getCount();

		if (moduleCount == 1) {
			listenedPartitions = allPartitions;
		}
		else {
			listenedPartitions = new ArrayList<Partition>();
			for (Partition partition : allPartitions) {
				// divide partitions across modules
				if (accessor.getPartitionIndex() != -1) {
					if ((partition.getId() % moduleCount) == accessor.getPartitionIndex()) {
						listenedPartitions.add(partition);
					}
				}
				else {
					int moduleSequence = accessor.getSequence();
					if (moduleCount == 0) {
						throw new IllegalArgumentException("This type of transport does not support 0-count modules");
					}
					if (moduleCount == 1) {
						listenedPartitions.add(partition);
					}
					else {
						// sequence numbers are zero-based
						if ((partition.getId() % moduleCount) == (moduleSequence - 1)) {
							listenedPartitions.add(partition);
						}
					}
				}
			}
		}

		final DirectChannel bridge = new DirectChannel();

		KafkaMessageListenerContainer messageListenerContainer =
				createMessageListenerContainer(group, concurrency, listenedPartitions,
						referencePoint);

		KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter =
				new KafkaMessageDrivenChannelAdapter(messageListenerContainer);
		kafkaMessageDrivenChannelAdapter.setBeanFactory(this.getBeanFactory());
		kafkaMessageDrivenChannelAdapter.setKeyDecoder(keyDecoder);
		kafkaMessageDrivenChannelAdapter.setPayloadDecoder(valueDecoder);
		kafkaMessageDrivenChannelAdapter.setOutputChannel(bridge);
		kafkaMessageDrivenChannelAdapter.afterPropertiesSet();
		kafkaMessageDrivenChannelAdapter.start();

		ReceivingHandler rh = new ReceivingHandler(kafkaMessageDrivenChannelAdapter,
				messageListenerContainer.getOffsetManager());
		rh.setOutputChannel(moduleInputChannel);
		EventDrivenConsumer edc = new EventDrivenConsumer(bridge, rh);
		edc.setBeanName("inbound." + name);

		Binding consumerBinding = Binding.forConsumer(name, edc, moduleInputChannel, accessor);
		addBinding(consumerBinding);
		consumerBinding.start();

	}

	public KafkaMessageListenerContainer createMessageListenerContainer(String group, int numThreads, String topic,
			long referencePoint) {
		return createMessageListenerContainer(group, numThreads, topic, null, referencePoint);
	}

	public KafkaMessageListenerContainer createMessageListenerContainer(String group, int numThreads,
			Collection<Partition> listenedPartitions, long referencePoint) {
		return createMessageListenerContainer(group, numThreads, null, listenedPartitions, referencePoint);
	}

	private KafkaMessageListenerContainer createMessageListenerContainer(String group, int numThreads, String topic,
			Collection<Partition> listenedPartitions, long referencePoint) {
		Assert.isTrue(StringUtils.hasText(topic) ^ !CollectionUtils.isEmpty(listenedPartitions),
				"Exactly one of topic or a list of listened partitions must be provided");
		KafkaMessageListenerContainer messageListenerContainer;
		if (topic != null) {
			messageListenerContainer = new KafkaMessageListenerContainer(connectionFactory, topic);
		}
		else {
			messageListenerContainer = new KafkaMessageListenerContainer(connectionFactory,
					listenedPartitions.toArray(new Partition[listenedPartitions.size()]));
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Listening to topic " + topic);
		}
		// if we have less target partitions than target concurrency, adjust accordingly
		messageListenerContainer.setConcurrency(Math.min(numThreads, listenedPartitions.size()));
		OffsetManager offsetManager = createOffsetManager(group, referencePoint);
		messageListenerContainer.setOffsetManager(offsetManager);
		messageListenerContainer.setQueueSize(defaultQueueSize);
		return messageListenerContainer;
	}

	private OffsetManager createOffsetManager(String group, long referencePoint) {
		try {
			KafkaTopicOffsetManager kafkaOffsetManager =
					new KafkaTopicOffsetManager(zookeeperConnect, offsetStoreTopic, Collections.<Partition, Long>emptyMap());
			kafkaOffsetManager.setConsumerId(group);
			kafkaOffsetManager.setReferenceTimestamp(referencePoint);
			kafkaOffsetManager.setSegmentSize(offsetStoreSegmentSize);
			kafkaOffsetManager.setRetentionTime(offsetStoreRetentionTime);
			kafkaOffsetManager.setRequiredAcks(offsetStoreRequiredAcks);
			kafkaOffsetManager.setMaxSize(offsetStoreMaxFetchSize);
			kafkaOffsetManager.setBatchWrites(offsetStoreBatchEnabled);
			kafkaOffsetManager.setMaxBatchSize(offsetStoreBatchSize);
			kafkaOffsetManager.setMaxQueueBufferingTime(offsetStoreBatchTime);

			kafkaOffsetManager.afterPropertiesSet();

			WindowingOffsetManager windowingOffsetManager = new WindowingOffsetManager(kafkaOffsetManager);
			windowingOffsetManager.setTimespan(offsetUpdateTimeWindow);
			windowingOffsetManager.setCount(offsetUpdateCount);
			windowingOffsetManager.setShutdownTimeout(offsetUpdateShutdownTimeout);

			windowingOffsetManager.afterPropertiesSet();
			return windowingOffsetManager;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private class KafkaPropertiesAccessor extends AbstractBusPropertiesAccessor {

		public KafkaPropertiesAccessor(Properties properties) {
			super(properties);
		}

		public int getNumberOfKafkaPartitionsForProducer() {
			int concurrency = getProperty(NEXT_MODULE_CONCURRENCY, defaultConcurrency);
			if (new PartitioningMetadata(this).isPartitionedModule()) {
				return getPartitionCount() * concurrency;
			}
			else {
				int nextModuleCount = getProperty(NEXT_MODULE_COUNT, 1);
				if (nextModuleCount == 0) {
					throw new IllegalArgumentException("Module count cannot be zero");
				}
				return nextModuleCount * concurrency;
			}
		}

		public String getCompressionCodec(String defaultValue) {
			return getProperty(COMPRESSION_CODEC, defaultValue);
		}

		public int getRequiredAcks(int defaultRequiredAcks) {
			return getProperty(REQUIRED_ACKS, defaultRequiredAcks);
		}

	}

	private class ReceivingHandler extends AbstractReplyProducingMessageHandler implements Lifecycle {

		private KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter;

		private OffsetManager offsetManager;

		public ReceivingHandler(KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter,
				OffsetManager offsetManager) {
			this.kafkaMessageDrivenChannelAdapter = kafkaMessageDrivenChannelAdapter;
			this.offsetManager = offsetManager;
			this.setBeanFactory(KafkaMessageBus.this.getBeanFactory());
		}

		@Override
		@SuppressWarnings("unchecked")
		protected Object handleRequestMessage(Message<?> requestMessage) {
			Message<?> theRequestMessage = requestMessage;
			try {
				theRequestMessage = embeddedHeadersMessageConverter.extractHeaders((Message<byte[]>) requestMessage);
			}
			catch (Exception e) {
				logger.error(EmbeddedHeadersMessageConverter.decodeExceptionMessage(requestMessage), e);
			}
			return deserializePayloadIfNecessary(theRequestMessage);
		}

		@Override
		public void start() {
		}

		@Override
		public void stop() {
			if (offsetManager instanceof DisposableBean) {
				try {
					((DisposableBean) offsetManager).destroy();
				}
				catch (Exception e) {
					logger.error("Error while closing the offset manager", e);
				}
			}
			kafkaMessageDrivenChannelAdapter.stop();
		}

		@Override
		public boolean isRunning() {
			return true;
		}

		@Override
		protected boolean shouldCopyRequestHeaders() {
			// prevent the message from being copied again in superclass
			return false;
		}
	}

	private class SendingHandler extends AbstractMessageHandler {

		private final MessageHandler delegate;

		private final PartitioningMetadata partitioningMetadata;

		private final AtomicInteger roundRobinCount = new AtomicInteger();

		private final String topicName;

		private final int numberOfKafkaPartitions;

		private int factor;


		private SendingHandler(MessageHandler delegate, String topicName,
				KafkaPropertiesAccessor properties, int numberOfPartitions) {
			this.delegate = delegate;
			this.topicName = topicName;
			this.numberOfKafkaPartitions = numberOfPartitions;
			this.partitioningMetadata = new PartitioningMetadata(properties);
			this.setBeanFactory(KafkaMessageBus.this.getBeanFactory());
			if (partitioningMetadata.isPartitionedModule()) {
				// ceiling division of total number of kafka partitions to the Spring XD partition count
				// a container with a certain partition index will listen to at most this number of
				// partitions, evenly distributed. E.g. for 8 kafka partitions and 3 partition indices
				// the factor is 3, partition 0 will listen to 0,3,6, etc.
				// this value will be used further fanning data out to the multiple Kafka partitions that
				// are assigned to a given partition index
				this.factor = (numberOfKafkaPartitions + partitioningMetadata.getPartitionCount() - 1)
						/ partitioningMetadata.getPartitionCount();
			}
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			Map<String, Object> additionalHeaders = new HashMap<String, Object>();

			int partition;
			if (partitioningMetadata.isPartitionedModule()) {
				// this is the logical partition in Spring XD - we need to fan out the messages further
				int springXdPartition = determinePartition(message, partitioningMetadata);
				// in order to do so, we will generate a number whose modulus when applied to total partition count
				// is equal to springXdPartition
				partition = (roundRobin() % factor) * partitioningMetadata.getPartitionCount() + springXdPartition;
			}
			else {
				// The value will be modulo-ed by numPartitions by Kafka itself
				partition = roundRobin();
			}
			additionalHeaders.put(PARTITION_HEADER, partition);
			additionalHeaders.put("messageKey", partition);
			additionalHeaders.put("topic", topicName);


			@SuppressWarnings("unchecked")
			Message<byte[]> transformed = (Message<byte[]>) serializePayloadIfNecessary(message,
					MimeTypeUtils.APPLICATION_OCTET_STREAM);
			transformed = getMessageBuilderFactory().fromMessage(transformed)
					.copyHeaders(additionalHeaders)
					.build();
			Message<?> messageToSend = embeddedHeadersMessageConverter.embedHeaders(transformed,
					KafkaMessageBus.this.headersToMap);
			Assert.isInstanceOf(byte[].class, messageToSend.getPayload());
			delegate.handleMessage(messageToSend);
		}

		private int roundRobin() {
			int result = roundRobinCount.incrementAndGet();
			if (result == Integer.MAX_VALUE) {
				roundRobinCount.set(0);
			}
			return result;
		}

	}

}
