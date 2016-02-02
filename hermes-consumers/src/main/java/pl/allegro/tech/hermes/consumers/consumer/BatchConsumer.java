package pl.allegro.tech.hermes.consumers.consumer;

import com.codahale.metrics.Timer;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionPolicy;
import pl.allegro.tech.hermes.common.kafka.offset.PartitionOffset;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.consumer.batch.BatchEcosystem;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatch;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatchFactory;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatchReceiver;
import pl.allegro.tech.hermes.consumers.consumer.offset.SubscriptionOffsetCommitQueues;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceiver;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageBatchSender;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageSendingResult;
import pl.allegro.tech.hermes.tracker.consumers.Trackers;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static com.github.rholder.retry.WaitStrategies.fibonacciWait;
import static com.github.rholder.retry.WaitStrategies.fixedWait;
import static java.lang.String.format;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

    private BatchEcosystem ecosystem;

    public BatchConsumer(MessageReceiver receiver,
                         MessageBatchSender sender,
                         MessageBatchFactory batchFactory,
                         MessageBatchWrapper messageBatchWrapper,
                         SubscriptionOffsetCommitQueues offsets,
                         HermesMetrics hermesMetrics,
                         Trackers trackers,
                         Subscription subscription) {
        this.receiver = new MessageBatchReceiver(receiver, batchFactory, messageBatchWrapper, hermesMetrics, trackers);
        this.sender = sender;
        this.batchFactory = batchFactory;
        this.offsets = offsets;
        this.subscription = subscription;
        this.hermesMetrics = hermesMetrics;
        this.ecosystem = new BatchEcosystem(hermesMetrics, trackers);
    }

    @Override
    public void run() {
        setThreadName();
        while (isConsuming()) {
            Optional<MessageBatch> inflight = Optional.empty();
            try {
                logger.info("Trying to create new batch for subscription {}", subscription.getId());
                inflight = of(receiver.next(subscription));
                inflight.ifPresent(batch -> {
                    logger.info("Delivering batch for subscription {}", subscription.getId());
                    deliver(batch, createRetryer(batch, subscription.getSubscriptionPolicy()));
                    logger.info("Finished delivering batch for subscription {}", subscription.getId());
                    offsets.putAllDelivered(batch.getPartitionOffsets());
                });
            } finally {
                logger.info("Cleaning batch for subscription {}", subscription.getId());
                inflight.ifPresent(this::clean);
            }
        }
        logger.info("Stopped consumer for subscription {}", subscription.getId());
        unsetThreadName();
        stoppedLatch.countDown();
    }

    private Retryer<MessageSendingResult> createRetryer(MessageBatch batch, SubscriptionPolicy policy) {
        return createRetryer(batch, policy.getMessageBackoff(), policy.getMessageTtl(), policy.isRetryClientErrors());
    }

    private Retryer<MessageSendingResult> createRetryer(final MessageBatch batch, int messageBackoff, int messageTtl, boolean retryClientErrors) {
        return RetryerBuilder.<MessageSendingResult>newBuilder()
                .retryIfExceptionOfType(IOException.class)
                .retryIfRuntimeException()
                .retryIfResult(result -> isConsuming() && !result.succeeded() && (!result.isClientError() || retryClientErrors))
                .withWaitStrategy(fixedWait(messageBackoff, MILLISECONDS))
                .withStopStrategy(attempt -> attempt.getDelaySinceFirstAttempt() > messageTtl)
                .withRetryListener(getRetryListener(result -> ecosystem.markFailed(batch, subscription, result)))
                .build();
    }

    private void deliver(MessageBatch batch, Retryer<MessageSendingResult> retryer) {
        try (Timer.Context timer = hermesMetrics.subscriptionLatencyTimer(subscription).time()) {
//            MessageSendingResult result = retryer.call(() -> sender.send(batch, subscription.getEndpoint()));
            MessageSendingResult result = sender.send(batch, subscription.getEndpoint());
            ecosystem.markSendingResult(batch, subscription, result);
        } catch (Exception e) {
            logger.error(format("[batch_id=%s, subscription=%s] Batch was rejected.", batch.getId(), subscription.toSubscriptionName()), e);
            ecosystem.markDiscarded(batch, subscription, e.getMessage());
        }
    }

    private void clean(MessageBatch batch) {
        batchFactory.destroyBatch(batch);
        ecosystem.closeInflightMetrics(batch, subscription);
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

    private RetryListener getRetryListener(java.util.function.Consumer<MessageSendingResult> consumer) {
        return new RetryListener() {
            @Override
            public <V> void onRetry(Attempt<V> attempt) {
                consumer.accept((MessageSendingResult) attempt.getResult());
            }
        };
    }
}
