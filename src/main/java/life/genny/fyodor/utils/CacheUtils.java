package life.genny.fyodor.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.transaction.Transactional;
import javax.json.JsonObject;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.json.JsonArray;

import org.jboss.logging.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.runtime.StartupEvent;
import life.genny.qwandaq.models.GennyToken;
import life.genny.qwandaq.utils.KeycloakUtils;
import life.genny.qwandaq.attribute.Attribute;
import life.genny.qwandaq.attribute.EntityAttribute;
import life.genny.qwandaq.datatype.DataType;
import life.genny.qwandaq.converter.ValidationListConverter;
import life.genny.qwandaq.entity.BaseEntity;
import life.genny.qwandaq.EEntityStatus;
import life.genny.qwandaq.validation.Validation;

@ApplicationScoped
public class CacheUtils {

	private static final Logger log = Logger.getLogger(CacheUtils.class);

	@ConfigProperty(name = "genny.keycloak.url", defaultValue = "https://keycloak.gada.io")
	String baseKeycloakUrl;

	@ConfigProperty(name = "genny.keycloak.realm", defaultValue = "genny")
	String keycloakRealm;

	@ConfigProperty(name = "genny.service.username", defaultValue = "service")
	String serviceUsername;

	@ConfigProperty(name = "genny.service.password", defaultValue = "password")
	String servicePassword;

	@ConfigProperty(name = "genny.oidc.client-id", defaultValue = "backend")
	String clientId;

	@ConfigProperty(name = "genny.oidc.credentials.secret", defaultValue = "secret")
	String secret;

	@ConfigProperty(name = "genny.service.cache.host")
	String host;

	@ConfigProperty(name = "genny.service.cache.port")
	Integer port;

	@ConfigProperty(name = "genny.service.cache.db", defaultValue = "false")
	Boolean cacheDB;

	@Inject
	EntityManager entityManager;

	GennyToken serviceToken;

	Jsonb jsonb = JsonbBuilder.create();

    static public Map<String,Map<String, Attribute>> realmAttributeMap = new ConcurrentHashMap<>();

    void onStart(@Observes StartupEvent ev) {
        log.info("The Cache is starting...");
		serviceToken = new KeycloakUtils().getToken(baseKeycloakUrl, keycloakRealm, clientId, secret, serviceUsername, servicePassword, null);
		loadAllAttributesFromCache();
    }

    public Attribute getAttribute(final String attributeCode) {

    	String realm = serviceToken.getRealm();

    	if (realmAttributeMap.get(serviceToken.getRealm())==null) {
    		loadAllAttributesFromCache();
    	}

        Attribute attribute = realmAttributeMap.get(realm).get(attributeCode);

		if (attribute == null) {
			log.error("Bad Attribute in Map for realm " +realm + " and code " + attributeCode);
		}

        return attribute;
    }


	public List<JsonArray> parseJsonRows(String jsonString) {

		List<JsonArray> response = new ArrayList<>();
		String item = "";
		Boolean active = false;
		Boolean readIn = false;

		for (int i = 0; i < jsonString.length(); i++) {
			char c = jsonString.charAt(i);

			if (c == '}') {
				active = true;
			}
			if (active) {

				if (c == '[') {
					readIn = true;
				}

				if (readIn) {
					item += c;
				}

				if (c == ']') {

					try {
						JsonArray row = jsonb.fromJson(item, JsonArray.class);
						response.add(row);
						// log.info(item);
						item = "";
						readIn = false;
					} catch (Exception e) { 
						continue;
					}
				}
			}
		}
		return response;
	}

	public int indexOf(JsonArray arr, String item) {
		for (int i = 0; i < arr.size(); i++) {
			if (arr.getString(i).equals(item)) {
				return i;
			}
		}
		return -1;
	}

	public String getString(JsonArray arr, String key, JsonArray columnNames) {
		int index = indexOf(columnNames, key);
		if (index > -1) {
			if (!arr.isNull(index)) {
				return arr.getString(index);
			}
		}
		return null;
	}

	public Integer getInteger(JsonArray arr, String key, JsonArray columnNames) {
		int index = indexOf(columnNames, key);
		if (index > -1) {
			if (!arr.isNull(index)) {
				return arr.getInt(index);
			}
		}
		return null;
	}

	public Boolean getBoolean(JsonArray arr, String key, JsonArray columnNames) {
		int index = indexOf(columnNames, key);
		if (index > -1) {
			if (!arr.isNull(index)) {
				return arr.getInt(index) == 1;
			}
		}
		return null;
	}

    public void loadAllAttributesFromCache() {

		String realm = serviceToken.getRealm();

		log.info("About to load all attributes for realm " + realm);

		List<Attribute> attributeList = null;

		if (cacheDB) {
			// Fetch from Database
			attributeList = fetchAttributesFromDB();

		} else {
			// Otherwise fetch from KSQLDB
			String requestBody = "{ \"sql\": \"SELECT * FROM ATTRIBUTE;\" }";

			String uri = host + ":" + port.toString() + "/query-stream";

			HttpClient client = HttpClient.newHttpClient();
			HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(uri))
				.setHeader("Content-Type", "application/json; charset=utf-8")
				.POST(HttpRequest.BodyPublishers.ofString(requestBody))
				.build();

			String body = null;

			try {
				HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
				body = response.body();
			} catch (IOException | InterruptedException e) {
				log.error(e.getLocalizedMessage());
			}

			if (body != null && !body.isBlank()) {
				attributeList = new ArrayList<>();

				JsonObject header = jsonb.fromJson(body, JsonObject.class);
				List<JsonArray> rows = parseJsonRows(body);

				JsonArray columnNames = header.getJsonArray("columnNames");

				for (JsonArray row : rows) {

					String code = getString(row, "CODE", columnNames);
					String name = getString(row, "NAME", columnNames);
					String className = getString(row, "CLASSNAME", columnNames);

					DataType dataType = new DataType(className);

					String inputMask = getString(row, "INPUTMASK", columnNames);
					String dttCode = getString(row, "DTTCODE", columnNames);
					String typeName = getString(row, "TYPENAME", columnNames);

					dataType.setInputmask(inputMask);
					dataType.setDttCode(dttCode);
					dataType.setTypeName(typeName);

					String vList = getString(row, "VALIDATION_LIST", columnNames);
					ValidationListConverter converter = new ValidationListConverter();
					List<Validation> validations = converter.convertToEntityAttribute(vList);
					dataType.setValidationList(validations);

					Attribute attribute = new Attribute(code, name, dataType);

					Long id = Long.valueOf(getInteger(row, "ID", columnNames));
					String defaultValue = getString(row, "DEFAULTVALUE", columnNames);
					Boolean defaultPrivacyFlag = getBoolean(row, "DEFAULTPRIVACYFLAG", columnNames);
					String description = getString(row, "DESCRIPTION", columnNames);
					String icon = getString(row, "ICON", columnNames);

					Integer statusOrdinal = getInteger(row, "STATUS", columnNames);
					if (statusOrdinal == null) {
						statusOrdinal = 0;
					}
					EEntityStatus status = EEntityStatus.values()[statusOrdinal];

					attribute.setId(id);
					attribute.setDefaultValue(defaultValue);
					attribute.setDefaultPrivacyFlag(defaultPrivacyFlag);
					attribute.setDescription(description);
					attribute.setIcon(icon);
					attribute.setStatus(status);

					attributeList.add(attribute);
				}

			} else {
				log.info("Failed to get data");
			}
		}

		if (attributeList == null) {
			log.error("Null attributeList, not putting in map!!!");
			return;
		}

		// Check for existing map
		if (!realmAttributeMap.containsKey(realm)) {
			realmAttributeMap.put(realm, new ConcurrentHashMap<String,Attribute>());
		}
		Map<String,Attribute> attributeMap = realmAttributeMap.get(realm);

		// Insert attributes into map
		for (Attribute attribute : attributeList) {
			attributeMap.put(attribute.getCode(), attribute);
		}

		String location = cacheDB ? "DB" : "KSQLDB";
		log.info("All attributes have been loaded in from " + location + ": " + attributeMap.size() + " attributes loaded!");
    }

	@Transactional
	public List<Attribute> fetchAttributesFromDB() throws NoResultException {

        try {

			final List<Attribute> results = entityManager.createQuery("SELECT a FROM Attribute a where a.realm=:realmStr and a.name not like 'App\\_%'")
					.setParameter("realmStr", serviceToken.getRealm())
					.getResultList();

			return results;

        } catch (NoResultException e) {
            log.error("No results found from DB search");
            e.printStackTrace();
		}
		return null;
	}

	/**
	* Get a BaseEntity from the KSQLDB cache
	*
	* @param code		Code to search by
	* @return			BaseEntity
	 */
	public BaseEntity getBaseEntityByCode(final String code) {
		// TODO: Complete this method
		return null;
	}

}
