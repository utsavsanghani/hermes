package pl.allegro.tech.hermes.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;

public class TopicWithSchema {

    @NotNull
    @Valid
    private Topic topic;

    private String schema;

    @JsonCreator
    public TopicWithSchema(@JsonProperty("topic") Topic topic, @JsonProperty("schema") String schema) {
        this.topic = topic;
        this.schema = schema;
    }

    public Topic getTopic() {
        return topic;
    }

    public String getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicWithSchema that = (TopicWithSchema) o;
        return Objects.equals(topic, that.topic) &&
                Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, schema);
    }
}
