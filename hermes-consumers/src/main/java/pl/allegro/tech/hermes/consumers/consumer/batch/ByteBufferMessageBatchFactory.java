package pl.allegro.tech.hermes.consumers.consumer.batch;

import pl.allegro.tech.hermes.api.Subscription;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.UUID;

import static java.lang.String.format;

public class ByteBufferMessageBatchFactory implements MessageBatchFactory {
    private final DirectBufferPool bufferPool;
    private final Clock clock;

    public ByteBufferMessageBatchFactory(int poolableSize, int maxPoolSize, Clock clock) {
        this.clock = clock;
        this.bufferPool = new DirectBufferPool(maxPoolSize, poolableSize, true);
    }

    @Override
    public MessageBatch createBatch(Subscription subscription) {
        try {
            final String id = UUID.randomUUID().toString();
            int capacity = subscription.getSubscriptionPolicy().getBatchVolume();
            ByteBuffer buffer = bufferPool.allocate(capacity);
            buffer.limit(capacity);
            switch (subscription.getContentType()) {
                case JSON:
                    return new JsonMessageBatch(id, buffer, subscription, clock);
                case AVRO:
                default:
                    throw new UnsupportedOperationException(format("Batching is not supported yet for contentType=%s", subscription.getContentType()));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroyBatch(MessageBatch batch) {
        bufferPool.deallocate(batch.getContent());
    }
}
