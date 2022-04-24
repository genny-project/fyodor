package life.genny.fyodor.endpoints;

import io.vertx.core.http.HttpServerRequest;
import java.util.List;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import life.genny.qwandaq.attribute.Attribute;
import life.genny.qwandaq.message.QDataAttributeMessage;
import life.genny.qwandaq.utils.DatabaseUtils;
import life.genny.qwandaq.utils.HttpUtils;
import life.genny.qwandaq.utils.SecurityUtils;
import life.genny.serviceq.Service;
import org.jboss.logging.Logger;
import org.jboss.resteasy.annotations.jaxrs.PathParam;




/**
 * Attribute --- Endpoints providing database attribute access
 *
 * @author jasper.robison@gada.io
 *
 */
@Path("/attribute")
public class Attributes {

	private static final Logger log = Logger.getLogger(Attributes.class);

	static Jsonb jsonb = JsonbBuilder.create();

	@Context
	HttpServerRequest request;

	@Inject
	DatabaseUtils databaseUtils;

	@Inject
	Service service;

	/**
	 * Read an item from the cache.
	 *
	 * @param key The key of the cache item
	 * @return The json item
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/{realm}/{code}")
	public Response read(@HeaderParam("Authorization") String token, @PathParam("realm") String realm, @PathParam("code") String code) {

		Boolean authorized = SecurityUtils.isAuthorizedToken(token);
		if (!authorized) {
			return Response.status(Response.Status.BAD_REQUEST)
					.entity(HttpUtils.error("Not authorized to make this request")).build();
		}

//
		Attribute attribute = databaseUtils.findAttributeByCode(realm, code);

		return Response.ok(attribute).build();
	}

	/**
	 * Read an item from the cache.
	 *
	 * @param key The key of the cache item
	 * @return The json item
	 */
	@GET
	@Path("/{realm}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response readAll(@HeaderParam("Authorization") String token, @PathParam("realm") String realm) {

		Boolean authorized = SecurityUtils.isAuthorizedToken(token);
		if (!authorized) {
			return Response.status(Response.Status.BAD_REQUEST)
					.entity(HttpUtils.error("Not authorized to make this request")).build();
		}

		//String realm = service.getServiceToken().getRealm();
		List<Attribute> attributes = databaseUtils.findAttributes(realm, 0, 10000, "");
		QDataAttributeMessage attributeMsg = new QDataAttributeMessage(attributes.toArray(new Attribute[0]));


		return Response.ok(attributeMsg).build();
	}



}
