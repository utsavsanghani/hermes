package pl.allegro.tech.hermes.management.api;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pl.allegro.tech.hermes.api.PatchData;
import pl.allegro.tech.hermes.api.TopicWithSchema;
import pl.allegro.tech.hermes.management.api.auth.ManagementRights;
import pl.allegro.tech.hermes.management.api.auth.Roles;
import pl.allegro.tech.hermes.management.domain.topic.CreatorRights;
import pl.allegro.tech.hermes.management.domain.topic.TopicWithSchemaService;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.status;
import static pl.allegro.tech.hermes.api.TopicName.fromQualifiedName;

@Component
@Path("/topics-with-schema")
@Api(value = "/topics-with-schema", description = "Operations on topics and schemas")
public class TopicsWithSchemaEndpoint {

    private final TopicWithSchemaService topicWithSchemaService;

    private final ManagementRights managementRights;


    @Autowired
    public TopicsWithSchemaEndpoint(TopicWithSchemaService topicWithSchemaService,
                                    ManagementRights managementRights) {
        this.topicWithSchemaService = topicWithSchemaService;
        this.managementRights = managementRights;
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/{topicName}")
    @ApiOperation(value = "Topic details with schema", httpMethod = HttpMethod.GET)
    public TopicWithSchema get(@PathParam("topicName") String qualifiedTopicName) {
        return topicWithSchemaService.getTopicWithSchema(fromQualifiedName(qualifiedTopicName));
    }

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @RolesAllowed(Roles.ANY)
    @ApiOperation(value = "Create topic and schema", httpMethod = HttpMethod.POST)
    public Response create(TopicWithSchema topicWithSchema, @Context ContainerRequestContext requestContext) {
        String createdBy = requestContext.getSecurityContext().getUserPrincipal().getName();
        CreatorRights isAllowedToManage = checkedTopic -> managementRights.isUserAllowedToManageTopic(checkedTopic, requestContext);
        topicWithSchemaService.createTopicWithSchema(topicWithSchema, createdBy, isAllowedToManage);
        return status(Response.Status.CREATED).build();
    }

    @PUT
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("/{topicName}")
    @RolesAllowed({Roles.TOPIC_OWNER, Roles.ADMIN})
    @ApiOperation(value = "Update topic and schema", httpMethod = HttpMethod.PUT)
    public Response update(@PathParam("topicName") String qualifiedTopicName, PatchData patch,
                           @Context SecurityContext securityContext) {
        String updatedBy = securityContext.getUserPrincipal().getName();
        topicWithSchemaService.updateTopicWithSchema(fromQualifiedName(qualifiedTopicName), patch, updatedBy);
        return status(Response.Status.OK).build();
    }

    @DELETE
    @Produces(APPLICATION_JSON)
    @Path("/{topicName}")
    @RolesAllowed({Roles.TOPIC_OWNER, Roles.ADMIN})
    @ApiOperation(value = "Remove topic", httpMethod = HttpMethod.DELETE)
    public Response remove(@PathParam("topicName") String qualifiedTopicName, @Context SecurityContext securityContext) {
        String removedBy = securityContext.getUserPrincipal().getName();
        topicWithSchemaService.removeTopicWithSchema(fromQualifiedName(qualifiedTopicName), removedBy);
        return status(Response.Status.OK).build();
    }
}
