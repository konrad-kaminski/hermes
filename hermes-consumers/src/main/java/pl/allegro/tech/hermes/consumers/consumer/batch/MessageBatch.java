package pl.allegro.tech.hermes.consumers.consumer.batch;

import pl.allegro.tech.hermes.api.ContentType;
import pl.allegro.tech.hermes.consumers.consumer.Message;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public interface MessageBatch {
    boolean isFull();

    void append(Message message) throws BufferOverflowException;

    boolean isReadyForDelivery();

    String getId();

    ContentType getContentType();

    ByteBuffer close();

    ByteBuffer getContent();

    boolean isTtlExceeded(long deliveryStartTime);
}
