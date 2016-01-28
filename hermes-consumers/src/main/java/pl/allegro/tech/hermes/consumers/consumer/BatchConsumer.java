package pl.allegro.tech.hermes.consumers.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.common.kafka.offset.PartitionOffset;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatchFactory;
import pl.allegro.tech.hermes.consumers.consumer.offset.SubscriptionOffsetCommitQueues;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceiver;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatch;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceivingTimeoutException;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageBatchSender;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageSendingResult;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

public class BatchConsumer implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(BatchConsumer.class);

    private final MessageReceiver receiver;
    private final MessageBatchSender sender;
    private final MessageBatchFactory batchFactory;

    private final Clock clock;

    private final SubscriptionOffsetCommitQueues offsets;
    private Subscription subscription;

    private final CountDownLatch stoppedLatch = new CountDownLatch(1);
    boolean consuming = true;

    ObjectMapper mapper = new ObjectMapper();

    public BatchConsumer(MessageReceiver receiver,
                         MessageBatchSender sender,
                         MessageBatchFactory batchFactory,
                         SubscriptionOffsetCommitQueues offsets,
                         Subscription subscription,
                         Clock clock) {
        this.receiver = receiver;
        this.sender = sender;
        this.batchFactory = batchFactory;
        this.offsets = offsets;
        this.subscription = subscription;
        this.clock = clock;
    }

    @Override
    public void run() {
        setThreadName();
        Optional<Message> inflight = Optional.empty();
        do {
            MessageBatch batch = batchFactory.createBatch(subscription);
            inflight = fillBatch(batch, inflight);
            batch.close();
            deliver(batch, clock.millis());
            offsets.putAll(batch.getPartitionOffsets());
            batchFactory.destroyBatch(batch);
        } while (isConsuming());
        logger.info("Stopped consumer for subscription {}", subscription.getId());
        unsetThreadName();
        stoppedLatch.countDown();
    }

    private Optional<Message> fillBatch(MessageBatch batch, Optional<Message> i) {
        Optional<Message> inflight = i;
        while (isConsuming() && !batch.isReadyForDelivery()) {
            try {
                Message message = inflight.isPresent()? inflight.get() : receiver.next();
                inflight = Optional.empty();
                byte[] data = getWrappedMessage(message);
                if (batch.canFit(data)) {
                    batch.append(data, new PartitionOffset(message.getKafkaTopic(), message.getOffset(), message.getPartition()));
                } else {
                    return Optional.of(message);
                }
            } catch (MessageReceivingTimeoutException ex) {
                // ignore
            }
        }
        return Optional.empty();
    }

    private byte[] getWrappedMessage(Message message) {
        try {
            Map<String, Object> map = new HashMap<>();
            map.put("message_id", message.getId());
            if(!message.getExternalMetadata().isEmpty()) map.put("metadata", message.getExternalMetadata());
            map.put("content", mapper.readValue(message.getData(), Map.class));
            return mapper.writeValueAsBytes(map);
        } catch (IOException e) {
            return message.getData();
        }
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
