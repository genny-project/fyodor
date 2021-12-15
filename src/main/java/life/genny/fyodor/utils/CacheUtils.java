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
import java.time.LocalDateTime;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

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
import life.genny.qwandaq.exception.BadDataException;
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

    public void loadAllAttributesFromCache() {

		String realm = serviceToken.getRealm();
		List<Attribute> attributeList = null;

		log.info("About to load all attributes for realm " + realm);

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

					Long id = getLong(row, "ID", columnNames);
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
	public List<Attribute> fetchAttributesFromDB() {

        try {

			return entityManager.createQuery("SELECT a FROM Attribute a where a.realm=:realmStr and a.name not like 'App\\_%'", Attribute.class)
					.setParameter("realmStr", serviceToken.getRealm())
					.getResultList();

        } catch (NoResultException e) {
            log.error("No results found from DB search");
            log.error(e.getStackTrace());
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

		String requestBody = "{ \"sql\": \"SELECT * FROM BASEENTITY_ATTRIBUTE WHERE BASEENTITYCODE=\'"+code+"\' ;\" }";

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
			log.info(body);
		} catch (IOException | InterruptedException e) {
			log.error(e.getLocalizedMessage());
		}

		if (body == null || body.isBlank()) {
			log.error("Could not fetch entity: " + code);
			return null;
		}

		JsonObject header = jsonb.fromJson(body, JsonObject.class);
		List<JsonArray> rows = parseJsonRows(body);

		if (rows.size() == 0) {
			log.error("No rows returned for KSQLDB entity query");
			return null;
		}

		JsonArray columnNames = header.getJsonArray("columnNames");

		String entityCode = getString(rows.get(0), "BASEENTITYCODE", columnNames);
		// String name = getString(rows.get(0), "NAME", columnNames);

		BaseEntity entity = new BaseEntity(entityCode, entityCode);

		// Integer statusOrdinal = getInteger(rows.get(0), "STATUS", columnNames);
		// if (statusOrdinal == null) {
		// 	statusOrdinal = 0;
		// }
		// EEntityStatus status = EEntityStatus.values()[statusOrdinal];
		// entity.setStatus(status);

		for (JsonArray row : rows) {

			String attributeCode = getString(row, "ATTRIBUTECODE", columnNames);
			Double weight = getDouble(row, "WEIGHT", columnNames);
			Boolean readOnly = getBoolean(row, "READONLY", columnNames);
			Boolean inferred = getBoolean(row, "INFERRED", columnNames);
			Boolean privacyFlag = getBoolean(row, "PRIVACYFLAG", columnNames);
			Boolean confirmationFlag = getBoolean(row, "CONFIRMATIONFLAG", columnNames);
			String icon = getString(row, "ICON", columnNames);

			String valueString = getString(row, "VALUESTRING", columnNames);
			Integer valueInteger = getInteger(row, "VALUEINTEGER", columnNames);
			Boolean valueBoolean = getBoolean(row, "VALUEBOOLEAN", columnNames);
			Long valueLong = getLong(row, "VALUELONG", columnNames);
			LocalDate valueDate = getDate(row, "VALUEDATE", columnNames);
			LocalDateTime valueDateTime = getDateTime(row, "VALUEDATETIME", columnNames);
			LocalTime valueTime = getTime(row, "VALUETIME", columnNames);

			Attribute attribute = getAttribute(attributeCode);
			if (attribute == null) {
				continue;
			}

			EntityAttribute entityAttribute = new EntityAttribute(entity, attribute, weight);

			entityAttribute.setWeight(weight);
			entityAttribute.setReadonly(readOnly);
			entityAttribute.setInferred(inferred);
			entityAttribute.setPrivacyFlag(privacyFlag);
			entityAttribute.setConfirmationFlag(confirmationFlag);
			entityAttribute.setIcon(icon);

			entityAttribute.setValueString(valueString);
			entityAttribute.setValueInteger(valueInteger);
			entityAttribute.setValueBoolean(valueBoolean);
			entityAttribute.setValueLong(valueLong);
			entityAttribute.setValueDate(valueDate);
			entityAttribute.setValueDateTime(valueDateTime);
			entityAttribute.setValueTime(valueTime);

			try {
				entity.addAttribute(entityAttribute);
			} catch (BadDataException e) {
				log.error(e.getStackTrace());
			}

		}
		return entity;

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

	public Long getLong(JsonArray arr, String key, JsonArray columnNames) {
		int index = indexOf(columnNames, key);
		if (index > -1) {
			if (!arr.isNull(index)) {
				return arr.getJsonNumber(index).longValue();
			}
		}
		return null;
	}

	public Double getDouble(JsonArray arr, String key, JsonArray columnNames) {
		int index = indexOf(columnNames, key);
		if (index > -1) {
			if (!arr.isNull(index)) {
				return arr.getJsonNumber(index).doubleValue();
			}
		}
		return null;
	}

	public LocalDate getDate(JsonArray arr, String key, JsonArray columnNames) {
		int index = indexOf(columnNames, key);
		if (index > -1) {
			if (!arr.isNull(index)) {
				return LocalDate.parse(arr.getString(index));
			}
		}
		return null;
	}

	public LocalDateTime getDateTime(JsonArray arr, String key, JsonArray columnNames) {
		int index = indexOf(columnNames, key);
		if (index > -1) {
			if (!arr.isNull(index)) {
				return LocalDateTime.parse(arr.getString(index));
			}
		}
		return null;
	}

	public LocalTime getTime(JsonArray arr, String key, JsonArray columnNames) {
		int index = indexOf(columnNames, key);
		if (index > -1) {
			if (!arr.isNull(index)) {
				return LocalTime.parse(arr.getString(index));
			}
		}
		return null;
	}

}
