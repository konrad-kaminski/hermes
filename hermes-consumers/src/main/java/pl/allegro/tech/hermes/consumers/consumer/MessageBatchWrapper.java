package pl.allegro.tech.hermes.consumers.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MessageBatchWrapper {

    private final ObjectMapper mapper;

    @Inject
    public MessageBatchWrapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public byte[] wrap(Message message) {
        try {
            Map<String, Object> map = new HashMap<>();
            map.put("message_id", message.getId());
            if(!message.getExternalMetadata().isEmpty()) map.put("metadata", message.getExternalMetadata());
            map.put("content", mapper.readValue(message.getData(), Map.class));
            return mapper.writeValueAsBytes(map);
        } catch (IOException e) {
            return message.getData();
        }
    }
}
