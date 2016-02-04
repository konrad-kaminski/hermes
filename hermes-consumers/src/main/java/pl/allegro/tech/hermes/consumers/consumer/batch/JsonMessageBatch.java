package pl.allegro.tech.hermes.consumers.consumer.batch;

import pl.allegro.tech.hermes.api.ContentType;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.common.kafka.KafkaTopicName;
import pl.allegro.tech.hermes.common.kafka.offset.PartitionOffset;
import pl.allegro.tech.hermes.tracker.consumers.MessageMetadata;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

public class JsonMessageBatch implements MessageBatch {
    private final Clock clock;

    private final int maxBatchTime;
    private final int batchSize;

    private final String id;
    private final ByteBuffer byteBuffer;
    private final List<MessageMetadata> metadata = new ArrayList<>();

    private int elements = 0;
    private long batchStart;
    private boolean closed = false;

    public JsonMessageBatch(String id, ByteBuffer buffer, int size, int batchTime, Clock clock) {
        this.id = id;
        this.clock = clock;
        this.maxBatchTime = batchTime;
        this.batchSize = size;
        this.byteBuffer = buffer;
    }

    public JsonMessageBatch(String id, ByteBuffer buffer, Subscription subscription, Clock clock) {
        this(id, buffer,
                subscription.getSubscriptionPolicy().getBatchSize(),
                subscription.getSubscriptionPolicy().getBatchTime(),
                clock);
    }

    @Override
    public boolean isFull() {
        return elements >= batchSize || byteBuffer.remaining() < 2;
    }

    @Override
    public void append(byte[] data, MessageMetadata MessageMetadata) {
        checkState(!closed, "Batch already closed.");
        if (!canFit(data)) throw new BufferOverflowException();
        if (elements == 0) batchStart = clock.millis();

        byteBuffer.put((byte) (elements == 0 ? '[' : ',')).put(data);
        metadata.add(MessageMetadata);
        elements++;
    }

    @Override
    public boolean canFit(byte[] data) {
        return byteBuffer.remaining() >= data.length + 2;
    }

    @Override
    public boolean isReadyForDelivery() {
        return closed || isFull() || (batchStart != 0 && getLifetime() > maxBatchTime);
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
    public MessageBatch close() {
        byteBuffer.put("]".getBytes());
        int position = byteBuffer.position();
        byteBuffer.position(0);
        byteBuffer.limit(position);
        this.closed = true;
        return this;
    }

    @Override
    public ByteBuffer getContent() {
        if (closed) {
            byteBuffer.position(0);
        }
        return byteBuffer;
    }

    @Override
    public List<PartitionOffset> getPartitionOffsets() {
        return metadata.stream()
                .map(m -> new PartitionOffset(KafkaTopicName.valueOf(m.getTopic()), m.getOffset(), m.getPartition()))
                .collect(Collectors.toList());
    }

    @Override
    public List<MessageMetadata> getMessagesMetadata() {
        return Collections.unmodifiableList(metadata);
    }

    @Override
    public int size() {
        return elements;
    }

    @Override
    public long getLifetime() {
        return clock.millis() - batchStart;
    }
}