package pl.allegro.tech.hermes.consumers.consumer.sender.http;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import pl.allegro.tech.hermes.api.EndpointAddress;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatch;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageBatchSender;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageSendingResult;

import java.io.IOException;

import static pl.allegro.tech.hermes.api.ContentType.AVRO;
import static pl.allegro.tech.hermes.common.http.MessageMetadataHeaders.BATCH_ID;
import static pl.allegro.tech.hermes.consumers.consumer.sender.http.AvroMediaType.AVRO_BINARY;

public class ApacheMessageBatchSender implements MessageBatchSender {
    private CloseableHttpClient client = HttpClients.createMinimal();

    @Override
    public MessageSendingResult send(MessageBatch batch, EndpointAddress address) {
        try {
            ContentType contentType = getMediaType(batch.getContentType());
            HttpPost httpPost = new HttpPost(address.getEndpoint());
            ByteBufferEntity entity = new ByteBufferEntity(batch.getContent(), contentType);
            httpPost.setEntity(entity);
            httpPost.addHeader(HTTP.CONN_KEEP_ALIVE, "true");
            httpPost.addHeader(BATCH_ID.getName(), batch.getId());
            httpPost.addHeader(HTTP.CONTENT_TYPE, contentType.getMimeType());
            CloseableHttpResponse response = client.execute(httpPost);
            return new MessageSendingResult(response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            return new MessageSendingResult(e);
        }
    }

    public ContentType getMediaType(pl.allegro.tech.hermes.api.ContentType contentType) {
        return AVRO.equals(contentType) ? ContentType.create(AVRO_BINARY) : ContentType.APPLICATION_JSON;
    }
}
