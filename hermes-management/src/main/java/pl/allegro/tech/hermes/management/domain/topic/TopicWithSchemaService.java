package pl.allegro.tech.hermes.management.domain.topic;

import org.springframework.stereotype.Component;
import pl.allegro.tech.hermes.api.PatchData;
import pl.allegro.tech.hermes.api.RawSchema;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.api.TopicWithSchema;
import pl.allegro.tech.hermes.domain.topic.TopicAlreadyExistsException;
import pl.allegro.tech.hermes.management.config.TopicProperties;
import pl.allegro.tech.hermes.management.domain.topic.schema.SchemaService;
import pl.allegro.tech.hermes.management.domain.topic.validator.TopicValidator;

import java.util.Map;
import java.util.Optional;

import static pl.allegro.tech.hermes.api.ContentType.AVRO;

@Component
public class TopicWithSchemaService {

    private final TopicService topicService;

    private final TopicValidator topicValidator;

    private final SchemaService schemaService;

    private final TopicProperties topicProperties;

    public TopicWithSchemaService(TopicService topicService,
                                  TopicValidator topicValidator,
                                  SchemaService schemaService,
                                  TopicProperties topicProperties) {
        this.topicService = topicService;
        this.topicValidator = topicValidator;
        this.schemaService = schemaService;
        this.topicProperties = topicProperties;
    }

    public TopicWithSchema getTopicWithSchema(TopicName topicName) {
        Topic topic = topicService.getTopicDetails(topicName);
        Optional<RawSchema> schema = Optional.empty();
        if (isAvroTopic(topic)) {
            schema = schemaService.getSchema(topicName.qualifiedName());
        }
        return new TopicWithSchema(topic, schema.map(RawSchema::value).orElse(null));
    }

    public void createTopicWithSchema(TopicWithSchema topicWithSchema, String createdBy, CreatorRights isAllowedToManage) {
        Topic topic = topicWithSchema.getTopic();
        String schema = topicWithSchema.getSchema();
        topicValidator.ensureCreatedTopicIsValid(topic, isAllowedToManage);
        if (topicService.topicExists(topic.getName())) {
            throw new TopicAlreadyExistsException(topic.getName());
        }
        if (isAvroTopic(topic)) {
            registerAvroSchema(topic, schema);
        }
        topicService.createTopic(topic, createdBy, isAllowedToManage);
    }

    private void registerAvroSchema(Topic topic, String schema) {
        boolean schemaAlreadyRegistered = schemaService.getSchema(topic.getQualifiedName()).isPresent();
        if (schemaAlreadyRegistered) {
            throw new TopicSchemaExistsException(topic.getQualifiedName());
        }
        schemaService.registerSchema(topic, schema, true);
    }

    public void removeTopicWithSchema(TopicName topicName, String removedBy) {
        Topic topic = topicService.getTopicDetails(topicName);
        topicService.removeTopic(topic, removedBy);
        if (isAvroTopic(topic) && topicProperties.isRemoveSchema()) {
            schemaService.deleteAllSchemaVersions(topicName.qualifiedName());
        }
    }

    public void updateTopicWithSchema(TopicName topicName, PatchData patch, String updatedBy) {
        withTopicPatchData(patch).ifPresent(data -> topicService.updateTopic(topicName, data, updatedBy));
        withExtractedSchema(patch).ifPresent(schema -> {
            schemaService.registerSchema(topicService.getTopicDetails(topicName), schema, true);
        });
    }

    @SuppressWarnings("unchecked")
    private Optional<PatchData> withTopicPatchData(PatchData patch) {
        return Optional.ofNullable(patch.getPatch().get("topic"))
                .map(o -> (Map<String, Object>) o)
                .map(PatchData::from);
    }

    private Optional<String> withExtractedSchema(PatchData patch) {
        return Optional.ofNullable(patch.getPatch().get("schema")).map(o -> (String) o);
    }

    private boolean isAvroTopic(Topic topic) {
        return AVRO.equals(topic.getContentType());
    }
}
