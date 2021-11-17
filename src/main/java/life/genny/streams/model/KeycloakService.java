package life.genny.streams.model;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;


@Path("/")
@RegisterRestClient
public interface KeycloakService {
   
    @POST
    @Path("/auth/realms/{realm}/protocol/openid-connect/token")
    @Produces("application/json")
    String getAccessToken(@PathParam("realm") final String realm, final String paramMapBody);
    


}
