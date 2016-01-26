package pl.allegro.tech.hermes.consumers.consumer.batch;

import org.eclipse.jetty.io.ArrayByteBufferPool;
import org.eclipse.jetty.io.ByteBufferPool;
import pl.allegro.tech.hermes.api.Subscription;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.UUID;

import static java.lang.String.format;

public class ByteBufferMessageBatchFactory implements MessageBatchFactory {
    private final ByteBufferPool bufferPool;
    private final Clock clock;

    public ByteBufferMessageBatchFactory(int minPoolSize, int increment, int maxPoolSize, Clock clock) {
        this.clock = clock;
        this.bufferPool = new ArrayByteBufferPool(minPoolSize, increment, maxPoolSize);
    }

    @Override
    public MessageBatch createBatch(Subscription subscription) {
        final String id = UUID.randomUUID().toString();
        int capacity = subscription.getSubscriptionPolicy().getBatchVolume();
        ByteBuffer buffer = bufferPool.acquire(capacity, true);
        buffer.limit(capacity);
        switch (subscription.getContentType()) {
            case JSON:
                return new JsonMessageBatch(id, buffer, subscription, clock);
            case AVRO:
            default:
                throw new UnsupportedOperationException(format("Batching is not supported yet for contentType=%s", subscription.getContentType()));
        }
    }
}
