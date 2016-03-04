package pl.allegro.tech.hermes.domain.topic.schema;

import pl.allegro.tech.hermes.api.Topic;

import java.util.List;
import java.util.Optional;

public interface SchemaVersionsRepository {

    default boolean schemaVersionExists(Topic topic, SchemaVersion version) {
        return versions(topic).contains(version);
    }

    default Optional<SchemaVersion> latestSchemaVersion(Topic topic) {
        List<SchemaVersion> versions = versions(topic);
        return versions.isEmpty() ? Optional.empty() : Optional.of(versions.get(0));
    }

    List<SchemaVersion> versions(Topic topic);

}
