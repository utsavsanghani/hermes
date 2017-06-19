package pl.allegro.tech.hermes.api.endpoints;

import pl.allegro.tech.hermes.api.PatchData;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.api.TopicWithSchema;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("topics-with-schema")
public interface TopicWithSchemaEndpoint {

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/{topicName}")
    Topic get(@PathParam("topicName") String qualifiedTopicName);

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    Response create(TopicWithSchema topicWithSchema);

    @PUT
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("/{topicName}")
    Response update(@PathParam("topicName") String qualifiedTopicName, PatchData patch);

    @DELETE
    @Produces(APPLICATION_JSON)
    @Path("/{topicName}")
    Response remove(@PathParam("topicName") String qualifiedTopicName);
}
