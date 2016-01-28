package pl.allegro.tech.hermes.consumers.consumer.batch;

import pl.allegro.tech.hermes.api.ContentType;
import pl.allegro.tech.hermes.common.kafka.offset.PartitionOffset;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;

public interface MessageBatch {
    boolean isFull();

    void append(byte[] data, PartitionOffset offset) throws BufferOverflowException;

    boolean canFit(byte[] data);

    boolean isReadyForDelivery();

    String getId();

    ContentType getContentType();

    ByteBuffer close();

    ByteBuffer getContent();

    boolean isTtlExceeded(long deliveryStartTime);

    List<PartitionOffset> getPartitionOffsets();
}
