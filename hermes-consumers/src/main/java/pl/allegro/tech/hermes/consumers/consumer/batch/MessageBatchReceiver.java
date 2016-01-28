package pl.allegro.tech.hermes.consumers.consumer.batch;

import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.consumers.consumer.Message;
import pl.allegro.tech.hermes.consumers.consumer.MessageBatchWrapper;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceiver;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceivingTimeoutException;

import java.util.ArrayDeque;
import java.util.Queue;

public class MessageBatchReceiver {
    private final MessageReceiver receiver;
    private final MessageBatchFactory batchFactory;
    private final MessageBatchWrapper messageBatchWrapper;
    private final Queue<Message> inflight;
    private boolean receiving = true;

    public MessageBatchReceiver(MessageReceiver receiver,
                                MessageBatchFactory batchFactory,
                                MessageBatchWrapper messageBatchWrapper) {
        this.receiver = receiver;
        this.batchFactory = batchFactory;
        this.messageBatchWrapper = messageBatchWrapper;
        this.inflight = new ArrayDeque<>(1);
    }

    public MessageBatch next(Subscription subscription) {
        MessageBatch batch = batchFactory.createBatch(subscription);
        while (isReceiving() && !batch.isReadyForDelivery()) {
            try {
                Message message = inflight.isEmpty() ? receiver.next() : inflight.poll();
                byte[] data = messageBatchWrapper.wrap(message);
                if (batch.canFit(data)) {
                    batch.append(data, message.getPartitionOffset());
                } else {
                    inflight.offer(message);
                    break;
                }
            } catch (MessageReceivingTimeoutException ex) {
                // ignore
            }
        }
        return batch.close();
    }

    private boolean isReceiving() {
        return receiving;
    }

    public void stop() {
        receiving = false;
    }
}
