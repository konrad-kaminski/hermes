package pl.allegro.tech.hermes.tracker.consumers;

public class BatchMessageMetadata extends MessageMetadata {
    private final String batchId;

    public BatchMessageMetadata(String messageId, String batchId, long offset, int partition, String topic, String subscription, long publishingTimestamp, long readingTimestamp) {
        super(messageId, offset, partition, topic, subscription, publishingTimestamp, readingTimestamp);
        this.batchId = batchId;
    }

    public String getBatchId() {
        return batchId;
    }
}
