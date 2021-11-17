package life.genny.streams;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import life.genny.models.GennyToken;
import life.genny.qwanda.attribute.Attribute;
import life.genny.qwanda.attribute.AttributeText;
import life.genny.qwanda.attribute.EntityAttribute;
import life.genny.qwanda.entity.BaseEntity;
import life.genny.qwanda.entity.SearchEntity;
import life.genny.qwanda.exception.BadDataException;
import life.genny.qwanda.message.QDataBaseEntityMessage;
import life.genny.qwandautils.MergeUtil;
import life.genny.utils.DefUtils;

@ApplicationScoped
public class TopologyProducer {

	private static final Logger log = Logger.getLogger(TopologyProducer.class);

	@Inject
	InternalProducer producer;

	@Inject
	DefUtils defUtils;

	@ConfigProperty(name = "genny.show.values", defaultValue = "false")
	Boolean showValues;

	@ConfigProperty(name = "genny.keycloak.url", defaultValue = "https://keycloak.gada.io")
	String baseKeycloakUrl;

	@ConfigProperty(name = "genny.keycloak.realm", defaultValue = "genny")
	String keycloakRealm;

	@ConfigProperty(name = "genny.service.username", defaultValue = "service")
	String serviceUsername;

	@ConfigProperty(name = "genny.service.password", defaultValue = "password")
	String servicePassword;

	@ConfigProperty(name = "quarkus.oidc.auth-server-url", defaultValue = "https://keycloak.genny.life/auth/realms/genny")
	String keycloakUrl;

	@ConfigProperty(name = "genny.oidc.client-id", defaultValue = "backend")
	String clientId;

	@ConfigProperty(name = "genny.oidc.credentials.secret", defaultValue = "secret")
	String secret;

	@ConfigProperty(name = "genny.api.url", defaultValue = "http://alyson.genny.life:8280")
	String apiUrl;

	GennyToken serviceToken;

	Jsonb jsonb = JsonbBuilder.create();

	static final String WEATHER_STATIONS_STORE = "weather-stations-store";
	static final String ATTRIBUTES_STORE = "attributes-store";

	static final String SEARCH_EVENTS_TOPIC = "search_events";
	static final String SEARCH_DATA_TOPIC = "search_data";

	@Produces
	public Topology buildTopology() {

		if (showValues) {
			log.info("service username :" + serviceUsername);
			log.info("service password :" + servicePassword);
			log.info("keycloakUrl      :" + keycloakUrl);
			log.info("BasekeycloakUrl  :" + baseKeycloakUrl);
			log.info("keycloak clientId:" + clientId);
			log.info("keycloak secret  :" + secret);
			log.info("keycloak realm   :" + keycloakRealm);
			log.info("api Url          :" + apiUrl);
		}

		try {
			serviceToken = getToken(serviceUsername, servicePassword);
			defUtils.loadAllAttributesIntoCache(serviceToken);
		} catch (IOException e) {
			log.error("Cannot obtain Service Token for " + keycloakUrl + " and " + keycloakRealm);
		}

		StreamsBuilder builder = new StreamsBuilder();

		// Read the input Kafka topic into a KStream instance.
		builder.stream("search_events", Consumed.with(Serdes.String(), Serdes.String()))
				.filter((k, v) -> performSearch(v))
				.to("search_data", Produced.with(Serdes.String(), Serdes.String()));

		return builder.build();
	}

	public Boolean performSearch(String data) {

		// TODO: perform the search here

		return true;
	}

	private GennyToken getToken(final String username, final String password) throws IOException {

		JsonObject keycloakResponseJson = getToken(baseKeycloakUrl, keycloakRealm, clientId, secret, username, password,
				null);
		String accessToken = keycloakResponseJson.getString("access_token");
		GennyToken token = new GennyToken(accessToken);
		return token;
	}

	public JsonObject getToken(String keycloakUrl, String realm, String clientId, String secret, String username,
			String password, String refreshToken) throws IOException {

		HashMap<String, String> postDataParams = new HashMap<>();
		postDataParams.put("Content-Type", "application/x-www-form-urlencoded");

		if (refreshToken == null) {
			postDataParams.put("username", username);
			postDataParams.put("password", password);
			if (showValues) {
				log.info("using username " + username);
				log.info("using password " + password);
				log.info("using client_id " + clientId);
				log.info("using client_secret " + secret);
			}
			postDataParams.put("grant_type", "password");
		} else {
			postDataParams.put("refresh_token", refreshToken);
			postDataParams.put("grant_type", "refresh_token");
			if (showValues) {
				log.info("using refresh token");
				log.info(refreshToken);
			}
		}

		postDataParams.put("client_id", clientId);
		if (!StringUtils.isBlank(secret)) {
			postDataParams.put("client_secret", secret);
		}

		String requestURL = keycloakUrl + "/auth/realms/" + realm + "/protocol/openid-connect/token";

		String postDataStr = getPostDataString(postDataParams);
		// String str =keycloakService.getAccessToken(realm, postDataStr);
		String str = performPostCall(requestURL, postDataParams);

		if (showValues) {
			log.info("keycloak auth url = " + requestURL);
			log.info(username + " token= " + str);
		}

		JsonObject json = jsonb.fromJson(str, JsonObject.class);
		return json;

	}

	public String performPostCall(String requestURL, HashMap<String, String> postDataParams) {

		URL url;
		String response = "";
		try {
			url = new URL(requestURL);

			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setReadTimeout(15000);
			conn.setConnectTimeout(15000);
			conn.setRequestMethod("POST");
			conn.setDoInput(true);
			conn.setDoOutput(true);

			OutputStream os = conn.getOutputStream();
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			writer.write(getPostDataString(postDataParams));

			writer.flush();
			writer.close();
			os.close();
			int responseCode = conn.getResponseCode();

			if (responseCode == HttpsURLConnection.HTTP_OK) {
				String line;
				BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				while ((line = br.readLine()) != null) {
					response += line;
				}
			} else {
				response = "";

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return response;
	}

	private String getPostDataString(HashMap<String, String> params) throws UnsupportedEncodingException {
		StringBuilder result = new StringBuilder();
		boolean first = true;
		for (Map.Entry<String, String> entry : params.entrySet()) {
			if (first)
				first = false;
			else
				result.append("&");

			result.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
			result.append("=");
			result.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
		}

		return result.toString();
	}

	public Boolean hasDropdown(final String attributeCode, final BaseEntity defBe) throws Exception {

		// Check if attribute code exists as a SER
		Optional<EntityAttribute> searchAtt = defBe.findEntityAttribute("SER_" + attributeCode); // SER_
		if (searchAtt.isPresent()) {
			// temporary enable check
			String serValue = searchAtt.get().getValueString();
			log.info("Attribute exists in " + defBe.getCode() + " for SER_" + attributeCode + " --> " + serValue);
			JsonObject serJson = jsonb.fromJson(serValue, JsonObject.class);
			if (serJson.containsKey("enabled")) {
				Boolean isEnabled = serJson.getBoolean("enabled");
				return isEnabled;
			} else {
				log.info("Attribute exists in " + defBe.getCode() + " for SER_" + attributeCode
						+ " --> but NOT enabled!");
				return true;
			}
		} else {
			log.info("No attribute exists in " + defBe.getCode() + " for SER_" + attributeCode);
		}
		return false;

	}

}
