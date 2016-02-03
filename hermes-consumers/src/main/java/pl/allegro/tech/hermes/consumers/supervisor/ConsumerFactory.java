package pl.allegro.tech.hermes.consumers.supervisor;

import org.eclipse.jetty.client.HttpClient;
import pl.allegro.tech.hermes.api.DeliveryType;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.consumer.BatchConsumer;
import pl.allegro.tech.hermes.consumers.consumer.Consumer;
import pl.allegro.tech.hermes.consumers.consumer.MessageBatchWrapper;
import pl.allegro.tech.hermes.consumers.consumer.SerialConsumer;
import pl.allegro.tech.hermes.consumers.consumer.ConsumerMessageSenderFactory;
import pl.allegro.tech.hermes.consumers.consumer.batch.ByteBufferMessageBatchFactory;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatchFactory;
import pl.allegro.tech.hermes.consumers.consumer.converter.MessageConverterResolver;
import pl.allegro.tech.hermes.consumers.consumer.offset.SubscriptionOffsetCommitQueues;
import pl.allegro.tech.hermes.consumers.consumer.rate.ConsumerRateLimitSupervisor;
import pl.allegro.tech.hermes.consumers.consumer.rate.ConsumerRateLimiter;
import pl.allegro.tech.hermes.consumers.consumer.rate.calculator.OutputRateCalculator;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceiver;
import pl.allegro.tech.hermes.consumers.consumer.receiver.ReceiverFactory;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageBatchSender;
import pl.allegro.tech.hermes.consumers.consumer.sender.http.ApacheMessageBatchSender;
import pl.allegro.tech.hermes.domain.topic.TopicRepository;
import pl.allegro.tech.hermes.tracker.consumers.Trackers;

import javax.inject.Inject;
import java.time.Clock;
import java.util.concurrent.Semaphore;

import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_INFLIGHT_SIZE;

public class ConsumerFactory {

    private final ConsumerRateLimitSupervisor consumerRateLimitSupervisor;
    private final OutputRateCalculator outputRateCalculator;
    private final ReceiverFactory messageReceiverFactory;
    private final HermesMetrics hermesMetrics;
    private final ConfigFactory configFactory;
    private final Trackers trackers;
    private final ConsumerMessageSenderFactory consumerMessageSenderFactory;
    private final Clock clock;
    private final TopicRepository topicRepository;
    private final MessageConverterResolver messageConverterResolver;
    private final MessageBatchWrapper messageBatchWrapper;
    private final MessageBatchFactory batchFactory;

    @Inject
    public ConsumerFactory(ReceiverFactory messageReceiverFactory,
                           HermesMetrics hermesMetrics,
                           ConfigFactory configFactory,
                           ConsumerRateLimitSupervisor consumerRateLimitSupervisor,
                           OutputRateCalculator outputRateCalculator,
                           Trackers trackers,
                           ConsumerMessageSenderFactory consumerMessageSenderFactory,
                           Clock clock,
                           TopicRepository topicRepository,
                           MessageConverterResolver messageConverterResolver,
                           MessageBatchWrapper messageBatchWrapper,
                           MessageBatchFactory byteBufferMessageBatchFactory) {

        this.messageReceiverFactory = messageReceiverFactory;
        this.hermesMetrics = hermesMetrics;
        this.configFactory = configFactory;
        this.consumerRateLimitSupervisor = consumerRateLimitSupervisor;
        this.outputRateCalculator = outputRateCalculator;
        this.trackers = trackers;
        this.consumerMessageSenderFactory = consumerMessageSenderFactory;
        this.clock = clock;
        this.topicRepository = topicRepository;
        this.messageConverterResolver = messageConverterResolver;
        this.messageBatchWrapper = messageBatchWrapper;
        this.batchFactory = byteBufferMessageBatchFactory;
    }

    Consumer createConsumer(Subscription subscription) {
        SubscriptionOffsetCommitQueues subscriptionOffsetCommitQueues = new SubscriptionOffsetCommitQueues(
                subscription, hermesMetrics, clock, configFactory);

        ConsumerRateLimiter consumerRateLimiter = new ConsumerRateLimiter(subscription, outputRateCalculator, hermesMetrics,
                consumerRateLimitSupervisor);

        Semaphore inflightSemaphore = new Semaphore(configFactory.getIntProperty(CONSUMER_INFLIGHT_SIZE));

        Topic topic = topicRepository.getTopicDetails(subscription.getTopicName());

        MessageReceiver messageReceiver = messageReceiverFactory.createMessageReceiver(topic, subscription);

        if (DeliveryType.BATCH == subscription.getSubscriptionPolicy().getDeliveryType()) {
            MessageBatchSender sender = new ApacheMessageBatchSender(
                    configFactory.getIntProperty(Configs.CONSUMER_BATCH_CONNECTION_TIMEOUT),
                    configFactory.getIntProperty(Configs.CONSUMER_BATCH_SOCKET_TIMEOUT));

            return new BatchConsumer(messageReceiver,
                    sender,
                    batchFactory,
                    messageBatchWrapper,
                    subscriptionOffsetCommitQueues,
                    hermesMetrics,
                    trackers,
                    subscription);
        } else {
            return new SerialConsumer(
                    messageReceiver,
                    hermesMetrics,
                    subscription,
                    consumerRateLimiter,
                    subscriptionOffsetCommitQueues,
                    consumerMessageSenderFactory.create(subscription, consumerRateLimiter, subscriptionOffsetCommitQueues, inflightSemaphore),
                    inflightSemaphore,
                    trackers,
                    messageConverterResolver,
                    topic);
        }
    }
}
