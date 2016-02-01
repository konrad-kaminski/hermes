package pl.allegro.tech.hermes.consumers.consumer;

import com.codahale.metrics.Timer;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionPolicy;
import pl.allegro.tech.hermes.common.kafka.offset.PartitionOffset;
import pl.allegro.tech.hermes.common.metric.Counters;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.common.metric.Meters;
import pl.allegro.tech.hermes.common.metric.Timers;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatch;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatchFactory;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatchReceiver;
import pl.allegro.tech.hermes.consumers.consumer.offset.SubscriptionOffsetCommitQueues;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceiver;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageBatchSender;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageSendingResult;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.rholder.retry.WaitStrategies.fibonacciWait;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static pl.allegro.tech.hermes.common.metric.Meters.*;

public class BatchConsumer implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(BatchConsumer.class);

    private final MessageBatchSender sender;
    private final MessageBatchFactory batchFactory;
    private final SubscriptionOffsetCommitQueues offsets;
    private final CountDownLatch stoppedLatch = new CountDownLatch(1);
    private final MessageBatchReceiver receiver;
    private final HermesMetrics hermesMetrics;

    private Subscription subscription;
    boolean consuming = true;

    public BatchConsumer(MessageReceiver receiver,
                         MessageBatchSender sender,
                         MessageBatchFactory batchFactory,
                         MessageBatchWrapper messageBatchWrapper,
                         SubscriptionOffsetCommitQueues offsets,
                         HermesMetrics hermesMetrics,
                         Subscription subscription) {
        this.receiver = new MessageBatchReceiver(receiver, batchFactory, messageBatchWrapper, hermesMetrics);
        this.sender = sender;
        this.batchFactory = batchFactory;
        this.offsets = offsets;
        this.subscription = subscription;
        this.hermesMetrics = hermesMetrics;
    }

    @Override
    public void run() {
        setThreadName();
        Retryer<MessageSendingResult> retryer = createRetryer(subscription.getSubscriptionPolicy());
        do {
            MessageBatch batch = receiver.next(subscription);

            Timer.Context timer = hermesMetrics.timer(Timers.SUBSCRIPTION_LATENCY, subscription.getTopicName(), subscription.getName()).time();
            deliver(batch, retryer);
            timer.stop();

            offsets.putAllDelivered(batch.getPartitionOffsets());
            batchFactory.destroyBatch(batch);

            hermesMetrics.decrementInflightCounter(subscription, batch.size());
            hermesMetrics.inflightTimeHistogram(subscription).update(System.currentTimeMillis() - batch.startTime());
        } while (isConsuming());
        logger.info("Stopped consumer for subscription {}", subscription.getId());
        unsetThreadName();
        stoppedLatch.countDown();
    }

    private void deliver(MessageBatch batch, Retryer<MessageSendingResult> retryer) {
        try {

            MessageSendingResult finalResult = retryer.call(() -> {
                MessageSendingResult result = sender.send(batch, subscription.getEndpoint());
                hermesMetrics.registerConsumerHttpAnswer(subscription, result.getStatusCode());
                return result;
            });

            if (finalResult.succeeded()) {
                markDelivered(batch);
            } else {
                markDiscarded(batch);
            }
        } catch (ExecutionException | RetryException e) {
            logger.error(format("[batch_id=%s, subscription=%s] Batch was rejected.", batch.getId(), subscription.toSubscriptionName()), e);
            markDiscarded(batch);
        }
    }

    private void markDelivered(MessageBatch batch) {
        hermesMetrics.meter(METER).mark(batch.size());
        hermesMetrics.meter(TOPIC_METER, subscription.getTopicName()).mark(batch.size());
        hermesMetrics.meter(SUBSCRIPTION_METER, subscription.getTopicName(), subscription.getName()).mark(batch.size());
        hermesMetrics.counter(Counters.DELIVERED, subscription.getTopicName(), subscription.getName()).inc(batch.size());
    }

    private void markDiscarded(MessageBatch batch) {
        hermesMetrics.counter(Counters.DISCARDED, subscription.getTopicName(), subscription.getName()).inc(batch.size());
        hermesMetrics.meter(Meters.DISCARDED_METER).mark(batch.size());
        hermesMetrics.meter(Meters.DISCARDED_TOPIC_METER, subscription.getTopicName()).mark(batch.size());
        hermesMetrics.meter(Meters.DISCARDED_SUBSCRIPTION_METER, subscription.getTopicName(), subscription.getName()).mark(batch.size());
        hermesMetrics.decrementInflightCounter(subscription);
    }

    private Retryer<MessageSendingResult> createRetryer(SubscriptionPolicy policy) {
        return createRetryer(policy.getMessageBackoff(), policy.getMessageTtl(), policy.isRetryClientErrors());
    }

    private Retryer<MessageSendingResult> createRetryer(int messageBackoff, int messageTtl, boolean retryClientErrors) {
        return RetryerBuilder.<MessageSendingResult>newBuilder()
                .retryIfExceptionOfType(IOException.class)
                .retryIfRuntimeException()
                .retryIfResult(result -> isConsuming() && !result.succeeded() && (!result.isClientError() || retryClientErrors))
                .withWaitStrategy(fibonacciWait(messageBackoff, messageTtl, MILLISECONDS))
                .withStopStrategy(attempt -> attempt.getDelaySinceFirstAttempt() > messageTtl)
                .build();
    }

    @Override
    public Subscription getSubscription() {
        return subscription;
    }

    @Override
    public void updateSubscription(Subscription modifiedSubscription) {
        this.subscription = modifiedSubscription;
    }

    @Override
    public void stopConsuming() {
        logger.info("Stopping consumer for subscription {}", subscription.getId());
        receiver.stop();
        consuming = false;
    }

    @Override
    public void waitUntilStopped() throws InterruptedException {
        stoppedLatch.await();
    }

    @Override
    public List<PartitionOffset> getOffsetsToCommit() {
        return offsets.getOffsetsToCommit();
    }

    @Override
    public boolean isConsuming() {
        return consuming;
    }
}
