package pl.allegro.tech.hermes.consumers.consumer.sender.http;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.ByteBufferContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.io.RuntimeIOException;
import pl.allegro.tech.hermes.api.ContentType;
import pl.allegro.tech.hermes.api.EndpointAddress;
import pl.allegro.tech.hermes.consumers.consumer.batch.MessageBatch;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageBatchSender;
import pl.allegro.tech.hermes.consumers.consumer.sender.MessageSendingResult;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static pl.allegro.tech.hermes.api.ContentType.AVRO;
import static pl.allegro.tech.hermes.common.http.MessageMetadataHeaders.MESSAGE_ID;
import static pl.allegro.tech.hermes.consumers.consumer.sender.http.AvroMediaType.AVRO_BINARY;

public class JettyMessageBatchSender implements MessageBatchSender {
    private final HttpClient client;
    private final long timeout;

    public JettyMessageBatchSender(HttpClient client, long timeout) {
        this.client = client;
        this.timeout = timeout;
        try {
            client.start();
        } catch (Exception e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public MessageSendingResult send(MessageBatch batch, EndpointAddress address) {
        try {
            String mediaType = getMediaType(batch.getContentType());
            Request request = client.newRequest(address.getEndpoint())
                    .method(HttpMethod.POST)
                    .header(HttpHeader.KEEP_ALIVE.toString(), "true")
                    .header(MESSAGE_ID.getName(), batch.getId())
                    .header(HttpHeader.CONTENT_TYPE.toString(), mediaType)
                    .timeout(timeout, TimeUnit.MILLISECONDS)
                    .content(new ByteBufferContentProvider(mediaType, batch.getContent()));
            return new MessageSendingResult(request.send().getStatus());
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            return new MessageSendingResult(e);
        }
    }

    public String getMediaType(ContentType contentType) {
        return AVRO.equals(contentType) ? AVRO_BINARY : APPLICATION_JSON;
    }
}
