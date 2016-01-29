package pl.allegro.tech.hermes.consumers.consumer;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionPolicy;
import pl.allegro.tech.hermes.common.kafka.offset.PartitionOffset;
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

import static com.github.rholder.retry.WaitStrategies.fibonacciWait;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BatchConsumer implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(BatchConsumer.class);

    private final MessageBatchSender sender;
    private final MessageBatchFactory batchFactory;
    private final SubscriptionOffsetCommitQueues offsets;
    private final CountDownLatch stoppedLatch = new CountDownLatch(1);
    private final MessageBatchReceiver receiver;

    private Subscription subscription;
    boolean consuming = true;

    public BatchConsumer(MessageReceiver receiver,
                         MessageBatchSender sender,
                         MessageBatchFactory batchFactory,
                         MessageBatchWrapper messageBatchWrapper,
                         SubscriptionOffsetCommitQueues offsets,
                         Subscription subscription) {
        this.receiver = new MessageBatchReceiver(receiver, batchFactory, messageBatchWrapper);
        this.sender = sender;
        this.batchFactory = batchFactory;
        this.offsets = offsets;
        this.subscription = subscription;
    }

    @Override
    public void run() {
        setThreadName();
        Retryer<MessageSendingResult> retryer = createRetryer(subscription.getSubscriptionPolicy());
        do {
            MessageBatch batch = receiver.next(subscription);
            deliver(batch, retryer);
            offsets.putAll(batch.getPartitionOffsets());
            batchFactory.destroyBatch(batch);
        } while (isConsuming());
        logger.info("Stopped consumer for subscription {}", subscription.getId());
        unsetThreadName();
        stoppedLatch.countDown();
    }

    private void deliver(MessageBatch batch, Retryer<MessageSendingResult> retryer) {
        try {
            retryer.call(() -> sender.send(batch, subscription.getEndpoint()));
        } catch (ExecutionException | RetryException e) {
            logger.error(format("[batch_id=%s, subscription=%s] Batch was rejected.", batch.getId(), subscription.toSubscriptionName()), e);
        }
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
