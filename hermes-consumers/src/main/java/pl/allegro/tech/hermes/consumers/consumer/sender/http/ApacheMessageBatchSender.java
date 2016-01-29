package pl.allegro.tech.hermes.consumers.consumer.sender.http;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import pl.allegro.tech.hermes.api.EndpointAddress;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatch;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageBatchSender;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageSendingResult;

import java.io.IOException;

public class ApacheMessageBatchSender implements MessageBatchSender {
    CloseableHttpClient client = HttpClients.createMinimal();

    @Override
    public MessageSendingResult send(MessageBatch message, EndpointAddress address) {
        try {
            HttpPost httpPost = new HttpPost(address.getEndpoint());
            ByteBufferEntity entity = new ByteBufferEntity(message.getContent(), ContentType.APPLICATION_JSON);
            httpPost.setEntity(entity);
            CloseableHttpResponse response = client.execute(httpPost);
            return new MessageSendingResult(response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            return new MessageSendingResult(e);
        }
    }
}
