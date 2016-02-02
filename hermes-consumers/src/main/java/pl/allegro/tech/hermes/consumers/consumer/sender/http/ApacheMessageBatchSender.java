package pl.allegro.tech.hermes.consumers.consumer.sender.http;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
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

    private final RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(5000)
            .setConnectionRequestTimeout(5000)
            .setSocketTimeout(5000)
            .build();

    @Override
    public MessageSendingResult send(MessageBatch batch, EndpointAddress address) {
        HttpPost httpPost = null;
        try {
            ContentType contentType = getMediaType(batch.getContentType());
            httpPost = new HttpPost(address.getEndpoint());
            ByteBufferEntity entity = new ByteBufferEntity(batch.getContent(), contentType);
            httpPost.setConfig(requestConfig);
            httpPost.setEntity(entity);
            httpPost.addHeader(HTTP.CONN_KEEP_ALIVE, "true");
            httpPost.addHeader(BATCH_ID.getName(), batch.getId());
            httpPost.addHeader(HTTP.CONTENT_TYPE, contentType.getMimeType());
            CloseableHttpResponse response = client.execute(httpPost);
            EntityUtils.consumeQuietly(response.getEntity());
            return new MessageSendingResult(response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            return new MessageSendingResult(e);
        } finally {
            if (httpPost != null) httpPost.releaseConnection();
        }
    }

    public ContentType getMediaType(pl.allegro.tech.hermes.api.ContentType contentType) {
        return AVRO.equals(contentType) ? ContentType.create(AVRO_BINARY) : ContentType.APPLICATION_JSON;
    }
}
