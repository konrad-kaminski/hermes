package pl.allegro.tech.hermes.consumers.consumer.batch;

import pl.allegro.tech.hermes.api.ContentType;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.consumers.consumer.Message;

import java.nio.ByteBuffer;
import java.time.Clock;

public class JsonMessageBatch implements MessageBatch {
    private final Clock clock;

    private final long creationTime;
    private final int maxBatchTime;
    private final int messageTtl;
    private final int batchSize;

    private final String id;

    private final ByteBuffer byteBuffer;

    private int elements = 0;

    public JsonMessageBatch(String id, ByteBuffer buffer, int size, int maxBatchTime, int messageTtl, Clock clock) {
        this.id = id;
        this.clock = clock;
        this.creationTime = clock.millis();
        this.maxBatchTime = maxBatchTime;
        this.messageTtl = messageTtl;
        this.batchSize = size;
        this.byteBuffer = buffer;
    }

    public JsonMessageBatch(String id, ByteBuffer buffer, Subscription subscription, Clock clock) {
        this(id, buffer,
                subscription.getSubscriptionPolicy().getBatchSize(),
                subscription.getSubscriptionPolicy().getBatchTime(),
                subscription.getSubscriptionPolicy().getMessageTtl(),
                clock);
    }

    @Override
    public boolean isFull() {
        return elements >= batchSize;
    }

    @Override
    public void append(Message message) {
        if (elements == 0) {
            byteBuffer.put("[".getBytes());
        } else {
            byteBuffer.put(",".getBytes());
        }
        byteBuffer.put(message.getData());
        elements++;
    }

    @Override
    public boolean isReadyForDelivery() {
        return isFull() || clock.millis() - creationTime > maxBatchTime;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ContentType getContentType() {
        return ContentType.JSON;
    }

    @Override
    public ByteBuffer close() {
        byteBuffer.put("]".getBytes());
        int position = byteBuffer.position();
        byteBuffer.position(0);
        byteBuffer.limit(position);
        return byteBuffer;
    }

    @Override
    public boolean isTtlExceeded(long deliveryStartTime) {
        return clock.millis() - deliveryStartTime > messageTtl;
    }
}