package pl.allegro.tech.hermes.consumers.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.common.kafka.offset.PartitionOffset;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatch;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatchFactory;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatchReceiver;
import pl.allegro.tech.hermes.consumers.consumer.offset.SubscriptionOffsetCommitQueues;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceiver;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageBatchSender;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageSendingResult;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class BatchConsumer implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(BatchConsumer.class);

    private final MessageBatchSender sender;
    private final MessageBatchFactory batchFactory;

    private final Clock clock;
    private final SubscriptionOffsetCommitQueues offsets;
    private Subscription subscription;

    private final CountDownLatch stoppedLatch = new CountDownLatch(1);
    boolean consuming = true;

    private MessageBatchReceiver receiver;

    public BatchConsumer(MessageReceiver receiver,
                         MessageBatchSender sender,
                         MessageBatchFactory batchFactory,
                         MessageBatchWrapper messageBatchWrapper,
                         SubscriptionOffsetCommitQueues offsets,
                         Subscription subscription,
                         Clock clock) {
        this.receiver = new MessageBatchReceiver(receiver, batchFactory, messageBatchWrapper);
        this.sender = sender;
        this.batchFactory = batchFactory;
        this.offsets = offsets;
        this.subscription = subscription;
        this.clock = clock;
    }

    @Override
    public void run() {
        setThreadName();
        do {
            MessageBatch batch = receiver.next(subscription);
            deliver(batch, clock.millis());
            offsets.putAll(batch.getPartitionOffsets());
            batchFactory.destroyBatch(batch);
        } while (isConsuming());
        logger.info("Stopped consumer for subscription {}", subscription.getId());
        unsetThreadName();
        stoppedLatch.countDown();
    }

    private void deliver(MessageBatch batch, long deliveryStartTime) {
        boolean isRetryRequired;
        do {
            MessageSendingResult result = sender.send(batch, subscription.getEndpoint());
            isRetryRequired = isRetryRequired(result);
        } while (isRetryRequired && !batch.isTtlExceeded(deliveryStartTime));
    }

    private boolean isRetryRequired(MessageSendingResult result) {
        return isConsuming() &&
                !result.succeeded() &&
                (!result.isClientError() || subscription.getSubscriptionPolicy().isRetryClientErrors());
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
