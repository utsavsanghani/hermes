package pl.allegro.tech.hermes.integration.management;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.api.ContentType;
import pl.allegro.tech.hermes.api.ErrorCode;
import pl.allegro.tech.hermes.api.PatchData;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.api.TopicWithSchema;
import pl.allegro.tech.hermes.integration.IntegrationTest;

import javax.ws.rs.core.Response;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.allegro.tech.hermes.integration.test.HermesAssertions.assertThat;
import static pl.allegro.tech.hermes.test.helper.avro.AvroUserSchemaLoader.load;
import static pl.allegro.tech.hermes.test.helper.builder.TopicBuilder.topic;

public class TopicWithSchemaManagementTest extends IntegrationTest {

    private static final String EXAMPLE_SCHEMA = "\"string\"";

    @Test
    public void shouldCreateTopicWithSchema() {
        // given
        String groupName = "createTopicWithSchemaGroup";
        String topicName = "topic";
        String qualifiedTopicName = groupName + "." + topicName;
        operations.createGroup(groupName);

        // when
        Response response = management.topicWithSchema().create(
                new TopicWithSchema(topic(qualifiedTopicName).withContentType(ContentType.AVRO).build(), EXAMPLE_SCHEMA));

        // then
        assertThat(response).hasStatus(Response.Status.CREATED);
        assertThat(management.topic().get(qualifiedTopicName)).isNotNull();
        assertThat(management.schema().get(qualifiedTopicName).readEntity(String.class)).isEqualTo(EXAMPLE_SCHEMA);
    }

    @Test
    public void shouldNotCreateTopicWhenSchemaIsInvalid() {
        // given
        String groupName = "createTopicWithInvalidSchemaGroup";
        String topicName = "topic";
        String qualifiedTopicName = groupName + "." + topicName;
        operations.createGroup(groupName);

        // when
        Response response = management.topicWithSchema().create(
                new TopicWithSchema(topic(qualifiedTopicName).withContentType(ContentType.AVRO).build(), "{"));

        // then
        assertThat(response).hasStatus(Response.Status.BAD_REQUEST);
        assertThat(management.schema().get(qualifiedTopicName)).hasStatus(Response.Status.NO_CONTENT);
        assertThat(management.topic().list(groupName, false)).isEmpty();
    }

    @Test
    public void shouldNotCreateTopicWhenSchemaIsEmpty() {
        // given
        String groupName = "createTopicWithInvalidSchemaGroup";
        String topicName = "topic";
        String qualifiedTopicName = groupName + "." + topicName;
        operations.createGroup(groupName);

        // when
        Response response = management.topicWithSchema().create(
                new TopicWithSchema(topic(qualifiedTopicName).withContentType(ContentType.AVRO).build(), null));

        // then
        assertThat(response).hasStatus(Response.Status.BAD_REQUEST);
        assertThat(management.schema().get(qualifiedTopicName)).hasStatus(Response.Status.NO_CONTENT);
        assertThat(management.topic().list(groupName, false)).isEmpty();
    }

    @Test
    public void shouldNotSaveSchemaWhenTopicIsInvalid() {
        // given
        String groupName = "createInvalidTopicWithSchemaGroup";
        String topicName = "topic";
        String qualifiedTopicName = groupName + "." + topicName;
        operations.createGroup(groupName);

        // when
        Response response = management.topicWithSchema().create(
                new TopicWithSchema(topic(qualifiedTopicName).withContentType(ContentType.AVRO).withMaxMessageSize(-20).build(), EXAMPLE_SCHEMA));

        // then
        assertThat(response).hasStatus(Response.Status.BAD_REQUEST);
        assertThat(management.schema().get(qualifiedTopicName)).hasStatus(Response.Status.NO_CONTENT);
        assertThat(management.topic().list(groupName, false)).isEmpty();
    }

    @Test
    public void shouldNotSaveSchemaWhenTopicPreviouslyExisted() {
        // given
        String groupName = "createAlreadyExistingTopicWithSchemaGroup";
        String topicName = "topic";
        String qualifiedTopicName = groupName + "." + topicName;
        Topic topic = topic(qualifiedTopicName).withContentType(ContentType.AVRO).build();
        operations.createGroup(groupName);

        // when
        Response response = management.topicWithSchema().create(new TopicWithSchema(topic, EXAMPLE_SCHEMA));

        // then
        assertThat(response).hasStatus(Response.Status.CREATED);

        // when
        Response response2 = management.topicWithSchema().create(new TopicWithSchema(topic, "{}"));

        // then
        assertThat(response2).hasStatus(Response.Status.BAD_REQUEST).hasErrorCode(ErrorCode.TOPIC_ALREADY_EXISTS);
        assertThat(management.schema().get(qualifiedTopicName).readEntity(String.class)).isEqualTo(EXAMPLE_SCHEMA);
        assertThat(management.topic().get(qualifiedTopicName)).isNotNull();
    }

    @Test
    public void shouldRemoveTopicWithoutRemovingSchemaAccordingToConfiguration() {
        // given
        String groupName = "topicWithSchemaForRemoval";
        String topicName = "topic";
        String qualifiedTopicName = groupName + "." + topicName;
        Topic topic = topic(qualifiedTopicName).withContentType(ContentType.AVRO).build();
        operations.createGroup(groupName);

        // when
        Response response = management.topicWithSchema().create(new TopicWithSchema(topic, EXAMPLE_SCHEMA));

        // then
        assertThat(response).hasStatus(Response.Status.CREATED);
        assertThat(management.topic().list(groupName, false)).contains(qualifiedTopicName);
        assertThat(management.schema().get(qualifiedTopicName).readEntity(String.class)).isEqualTo(EXAMPLE_SCHEMA);

        // when
        Response removalResponse = management.topicWithSchema().remove(qualifiedTopicName);

        // then
        assertThat(removalResponse).hasStatus(Response.Status.OK);
        assertThat(management.schema().get(qualifiedTopicName).readEntity(String.class)).isEqualTo(EXAMPLE_SCHEMA);
        assertThat(management.topic().list(groupName, false)).isEmpty();
    }

    @Test
    public void shouldUpdateTopicAndSchema() {
        // given
        Schema schema1 = load("/schema/user.avsc");
        Schema schema2 = load("/schema/user_v2.avsc");
        String groupName = "createAndUpdateTopicWithSchemaGroup";
        String topicName = "topic";
        String qualifiedTopicName = groupName + "." + topicName;
        Topic topic = topic(qualifiedTopicName).withContentType(ContentType.AVRO).withMaxMessageSize(2048).build();
        operations.createGroup(groupName);

        // when
        Response response = management.topicWithSchema().create(new TopicWithSchema(topic, schema1.toString()));

        // then
        assertThat(response).hasStatus(Response.Status.CREATED);

        // when
        PatchData patchData = PatchData.from(ImmutableMap.of("topic", ImmutableMap.of("maxMessageSize", 1024), "schema", schema2.toString()));
        Response response2 = management.topicWithSchema().update(qualifiedTopicName, patchData);

        // then
        assertThat(response2).hasStatus(Response.Status.OK);
        assertThat(management.topic().get(qualifiedTopicName).getMaxMessageSize()).isEqualTo(1024);
        assertThat(management.schema().get(qualifiedTopicName).readEntity(String.class)).isEqualTo(schema2.toString());
    }
}
