package life.genny.fyodor.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Comparator;
import java.util.Collections;
import java.time.*;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Arrays;
import java.util.Optional;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.persistence.EntityManager;

import org.apache.commons.lang3.StringUtils;
import org.jboss.logging.Logger;

import io.quarkus.runtime.StartupEvent;

import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.querydsl.core.types.dsl.DateTimePath;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Predicate;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.JPQLQuery;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import life.genny.qwandaq.attribute.Attribute;
import life.genny.qwandaq.attribute.EntityAttribute;
import life.genny.qwandaq.entity.SearchEntity;
import life.genny.qwandaq.attribute.QEntityAttribute;
import life.genny.qwandaq.datatype.DataType;
import life.genny.qwandaq.entity.BaseEntity;
import life.genny.qwandaq.entity.QBaseEntity;
import life.genny.qwandaq.EEntityStatus;
import life.genny.qwandaq.entity.QEntityEntity;
import life.genny.qwandaq.message.QSearchBeResult;
import life.genny.qwandaq.message.QBulkMessage;
import life.genny.qwandaq.message.QDataBaseEntityMessage;
import life.genny.qwandaq.models.GennyToken;
import life.genny.qwandaq.Answer;
import life.genny.qwandaq.exception.BadDataException;
import life.genny.qwandaq.utils.BaseEntityUtils;
import life.genny.qwandaq.utils.CacheUtils;
import life.genny.qwandaq.utils.KeycloakUtils;
import life.genny.qwandaq.utils.QwandaUtils;

@ApplicationScoped
public class SearchUtility {

	private static final Logger log = Logger.getLogger(SearchUtility.class);

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

	@ConfigProperty(name = "genny.service.cache.db", defaultValue = "false")
	Boolean cacheDB;

	@Inject
	EntityManager entityManager;

	@Inject
	QwandaUtils qwandaUtils;

	GennyToken serviceToken;

	BaseEntityUtils beUtils;

	Jsonb jsonb = JsonbBuilder.create();

	static public Map<String, Map<String, Attribute>> realmAttributeMap = new ConcurrentHashMap<>();

	void onStart(@Observes StartupEvent ev) {
		serviceToken = KeycloakUtils.getToken(baseKeycloakUrl, keycloakRealm, clientId, secret, serviceUsername,
				servicePassword);

		// Init Utility Objects
		beUtils = new BaseEntityUtils(serviceToken);
	}

	public QBulkMessage processSearchEntity(SearchEntity searchBE, GennyToken userToken) {

		QSearchBeResult results = null;
		Boolean isCountEntity = false;

		// check if it is a count SBE
		if (searchBE.getCode().startsWith("CNS_")) {

			log.info("Found Count Entity " + searchBE.getCode());
			// Remove CNS_ prefix and set count var
			searchBE.setCode(searchBE.getCode().substring(4));
			isCountEntity = true;
		}

		// Check for a specific item search
		for (EntityAttribute attr : searchBE.getBaseEntityAttributes()) {
			if (attr.getAttributeCode().equals("PRI_CODE") && attr.getAttributeName().equals("_EQ_")) {
				log.info("SINGLE BASE ENTITY SEARCH DETECTED");

				BaseEntity be = beUtils.getBaseEntityByCode(attr.getValue());
				be.setIndex(0);
				BaseEntity[] arr = new BaseEntity[1];
				arr[0] = be;
				results = new QSearchBeResult(arr, Long.valueOf(1));
				break;
			}
		}

		// Perform search
		if (results == null) {
			results = findBySearch25(searchBE, isCountEntity, true);
		}

		List<EntityAttribute> cals = searchBE.findPrefixEntityAttributes("COL__");
		if (cals != null) {
			log.info("searchUsingSearch25 -> detected " + cals.size() + " CALS");

			for (EntityAttribute calEA : cals) {
				log.info("Found CAL with code: " + calEA.getAttributeCode());
			}
		}

		// Find Allowed Columns
		List<String> allowed = getSearchColumnFilterArray(searchBE);
		// Used to disable the column privacy
		EntityAttribute columnWildcard = searchBE.findEntityAttribute("COL_*").orElse(null);

		// Otherwise handle cals
		if (results != null && results.getEntities() != null && results.getEntities().length > 0) {

			for (BaseEntity be : results.getEntities()) {

				if (be != null) {

					// Filter unwanted attributes
					if (columnWildcard == null) {
						be = privacyFilter(be, allowed);
						be = handleSpecialAttributes(be, allowed);
					}

					for (EntityAttribute calEA : cals) {

						Answer ans = getAssociatedColumnValue(be, calEA.getAttributeCode());
						if (ans != null) {
							try {
								be.addAnswer(ans);
							} catch (BadDataException e) {
								log.error(e.getStackTrace());
							}
						}
					}
				}
			}
		}

		// Perform count for any combined search attributes
		Long totalResultCount = 0L;
		for (EntityAttribute ea : searchBE.getBaseEntityAttributes()) {
			if (ea.getAttributeCode().startsWith("CMB_")) {
				String combinedSearchCode = ea.getAttributeCode().substring("CMB_".length());
				SearchEntity combinedSearch = CacheUtils.getObject(this.serviceToken.getRealm(), combinedSearchCode,
						SearchEntity.class);

				Long subTotal = performCount(combinedSearch);
				if (subTotal != null) {
					totalResultCount += subTotal;
					results.setTotal(totalResultCount);
				} else {
					log.info("subTotal count for " + combinedSearchCode + " is NULL");
				}
			}
		}

		try {
			Attribute attrTotalResults = qwandaUtils.getAttribute("PRI_TOTAL_RESULTS");
			searchBE.addAnswer(new Answer(searchBE, searchBE, attrTotalResults, results.getTotal() + ""));
		} catch (BadDataException e) {
			log.error(e.getStackTrace());
		}

		log.info("Results = " + results.getTotal().toString());

		QBulkMessage bulkMsg = new QBulkMessage();
		bulkMsg.setToken(userToken.getToken());

		QDataBaseEntityMessage searchBEMsg = new QDataBaseEntityMessage(searchBE);
		searchBEMsg.setToken(userToken.getToken());
		searchBEMsg.setReplace(true);
		bulkMsg.add(searchBEMsg);

		// don't add result entities if it is only a count
		if (!isCountEntity) {
			QDataBaseEntityMessage entityMsg = new QDataBaseEntityMessage(results.getEntities());
			entityMsg.setTotal(results.getTotal());
			entityMsg.setReplace(true);
			entityMsg.setParentCode(searchBE.getCode());
			entityMsg.setToken(userToken.getToken());
			bulkMsg.add(entityMsg);
		}

		return bulkMsg;
	}

	public Long performCount(SearchEntity searchBE) {

		QSearchBeResult results = findBySearch25(searchBE, true, false);
		Long total = results.getTotal();

		// Perform count for any combined search attributes
		for (EntityAttribute ea : searchBE.getBaseEntityAttributes()) {
			if (ea.getAttributeCode().startsWith("CMB_")) {
				String combinedSearchCode = ea.getAttributeCode().substring("CMB_".length());
				SearchEntity combinedSearch = CacheUtils.getObject(this.serviceToken.getRealm(), combinedSearchCode,
						SearchEntity.class);
				Long subTotal = performCount(combinedSearch);
				if (subTotal != null) {
					total += subTotal;
				} else {
					log.info("subTotal count for " + combinedSearchCode + " is NULL");
				}
			}
		}
		return total;
	}

	/**
	 * Perform a safe search using named parameters to
	 * protect from SQL Injection
	 * 
	 * @param realm         Realm to search in.
	 * @param searchBE      SearchEntity used to search.
	 * @param countOnly     Only perform a count.
	 * @param fetchEntities Fetch Entities, or only codes.
	 * @return Search Result Object.
	 */
	public QSearchBeResult findBySearch25(final SearchEntity searchBE, Boolean countOnly, Boolean fetchEntities) {

		Instant start = Instant.now();

		log.info("About to search (" + searchBE.getCode() + ")");

		String realm = this.serviceToken.getRealm();
		Integer defaultPageSize = 20;
		// Init necessary vars
		QSearchBeResult result = null;
		List<String> codes = new ArrayList<String>();
		// Get page start and page size from SBE
		Integer pageStart = searchBE.getPageStart(0);
		Integer pageSize = searchBE.getPageSize(defaultPageSize);
		// Integer pageSize = searchBE.getPageSize(defaultPageSize);

		QBaseEntity baseEntity = new QBaseEntity("baseEntity");
		JPAQuery<?> query = new JPAQuery<Void>(entityManager);
		query.from(baseEntity);

		// Define a join for link searches
		String linkCode = null;
		String linkValue = null;
		String sourceCode = null;
		String targetCode = null;

		List<EntityAttribute> sortAttributes = new ArrayList<>();

		// Find AND and OR attributes and remove these prefixs from each of them
		List<EntityAttribute> andAttributes = searchBE.findPrefixEntityAttributes("AND_");
		List<EntityAttribute> orAttributes = searchBE.findPrefixEntityAttributes("OR_");

		String[] wildcardWhiteList = searchBE.findPrefixEntityAttributes("WTL_").stream()
				.map(x -> x.getAttributeCode().substring(4)).toArray(String[]::new);
		String[] wildcardBlackList = searchBE.findPrefixEntityAttributes("BKL_").stream()
				.map(x -> x.getAttributeCode().substring(4)).toArray(String[]::new);

		BooleanBuilder builder = new BooleanBuilder();

		// Ensure only Entities from our realm are returned
		log.info("realm is " + realm);
		builder.and(baseEntity.realm.eq(realm));

		// Default Status level is ACTIVE
		EEntityStatus status = EEntityStatus.ACTIVE;
		Integer joinCounter = 0;

		for (EntityAttribute ea : searchBE.getBaseEntityAttributes()) {

			final String attributeCode = ea.getAttributeCode();

			// Create where condition for the BE Code Filter
			if (attributeCode.equals("PRI_CODE")) {
				log.info("PRI_CODE like " + ea.getAsString());

				BooleanBuilder entityCodeBuilder = new BooleanBuilder();
				entityCodeBuilder.and(baseEntity.code.like(ea.getAsString()));

				// Process any AND/OR filters for BaseEntity Code
				andAttributes.stream()
						.filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode))
						.forEach(x -> {
							log.info("AND " + attributeCode + " like " + x.getAsString());
							entityCodeBuilder.and(baseEntity.code.like(x.getAsString()));
						});
				orAttributes.stream()
						.filter(x -> removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode))
						.forEach(x -> {
							log.info("OR " + attributeCode + " like " + x.getAsString());
							entityCodeBuilder.or(baseEntity.code.like(x.getAsString()));
						});
				builder.and(entityCodeBuilder);

				// Handle Created Date Filters
			} else if (attributeCode.startsWith("PRI_CREATED")) {
				builder.and(getDateTimePredicate(ea, baseEntity.created));
				// Process any AND/OR filters for this attribute
				andAttributes.stream()
						.filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode))
						.forEach(x -> {
							builder.and(getDateTimePredicate(x, baseEntity.created));
						});
				orAttributes.stream()
						.filter(x -> removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode))
						.forEach(x -> {
							builder.or(getDateTimePredicate(x, baseEntity.created));
						});

				// Handle Updated Date Filters
			} else if (attributeCode.startsWith("PRI_UPDATED")) {
				builder.and(getDateTimePredicate(ea, baseEntity.updated));
				// Process any AND/OR filters for this attribute
				andAttributes.stream()
						.filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode))
						.forEach(x -> {
							builder.and(getDateTimePredicate(x, baseEntity.updated));
						});
				orAttributes.stream()
						.filter(x -> removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode))
						.forEach(x -> {
							builder.or(getDateTimePredicate(x, baseEntity.updated));
						});

				// Create a Join for each attribute filters
			} else if ((attributeCode.startsWith("PRI_") || attributeCode.startsWith("LNK_"))
					&& !attributeCode.equals("PRI_CODE") && !attributeCode.equals("PRI_TOTAL_RESULTS")
					&& !attributeCode.startsWith("PRI_CREATED") && !attributeCode.startsWith("PRI_UPDATED")
					&& !attributeCode.equals("PRI_INDEX")) {

				// Generally don't accept filter LIKE "%", unless other filters present for this
				// attribute
				Boolean isAnyStringFilter = false;
				try {
					if (ea.getValueString() != null && "%".equals(ea.getValueString())
							&& "LIKE".equals(ea.getAttributeName())) {
						isAnyStringFilter = true;
					}
				} catch (Exception e) {
					log.error("Bad Null [" + ea + "]" + e.getLocalizedMessage());
				}

				String filterName = "eaFilterJoin_" + joinCounter.toString();
				QEntityAttribute eaFilterJoin = new QEntityAttribute(filterName);
				joinCounter++;

				BooleanBuilder currentAttributeBuilder = new BooleanBuilder();
				BooleanBuilder extraFilterBuilder = new BooleanBuilder();

				Boolean orTrigger = false;

				String joinAttributeCode = attributeCode;
				if (attributeCode.contains(".")) {
					// Ensure we now join on this LNK attr
					joinAttributeCode = attributeCode.split("\\.")[0];
					// Create a new set of filters just for this subquery
					List<EntityAttribute> subQueryEaList = searchBE.getBaseEntityAttributes().stream()
							.filter(x -> (removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode)
									|| removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode)))
							.collect(Collectors.toList());

					// Prepare for subquery by removing base attribute codes
					detatchBaseAttributeCode(subQueryEaList);
					// Must strip value to get clean code
					currentAttributeBuilder.and(
							Expressions.stringTemplate("replace({0},'[\"','')",
									Expressions.stringTemplate("replace({0},'\"]','')", eaFilterJoin.valueString))
									.in(generateSubQuery(subQueryEaList)));
				} else {
					currentAttributeBuilder.and(getAttributeSearchColumn(ea, eaFilterJoin));

					// Process any AND/OR filters for this attribute
					andAttributes.stream()
							.filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode))
							.forEach(x -> {
								extraFilterBuilder.and(getAttributeSearchColumn(x, eaFilterJoin));
							});
					// Using Standard for-loop to allow updating trigger variable
					for (EntityAttribute x : orAttributes) {
						if (removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode)) {
							extraFilterBuilder.or(getAttributeSearchColumn(x, eaFilterJoin));
							orTrigger = true;
						}
					}
				}

				// This should get around the bug that occurs with filter LIKE "%"
				if (!isAnyStringFilter) {

					query.leftJoin(eaFilterJoin)
							.on(eaFilterJoin.pk.baseEntity.id.eq(baseEntity.id)
									.and(eaFilterJoin.attributeCode.eq(joinAttributeCode)));

					if (extraFilterBuilder.hasValue()) {
						if (orTrigger) {
							currentAttributeBuilder.or(extraFilterBuilder);
						} else {
							currentAttributeBuilder.and(extraFilterBuilder);
						}
					}
					builder.and(currentAttributeBuilder);
				}
				// Create a filter for wildcard
			} else if (attributeCode.startsWith("SCH_WILDCARD")) {
				if (ea.getValueString() != null) {
					if (!StringUtils.isBlank(ea.getValueString())) {
						String wildcardValue = ea.getValueString();
						// wildcardValue = wildcardValue.replaceAll("[^A-zA-Z0-9 .,'@()_-]", "");
						wildcardValue = "%" + wildcardValue + "%";
						log.info("WILDCARD like " + wildcardValue);

						QEntityAttribute eaWildcardJoin = new QEntityAttribute("eaWildcardJoin");
						query.leftJoin(eaWildcardJoin);

						log.info("Whitelist = " + Arrays.toString(wildcardWhiteList));
						log.info("Blacklist = " + Arrays.toString(wildcardBlackList));
						query.on(eaWildcardJoin.pk.baseEntity.id.eq(baseEntity.id));

						// Find the depth level for associated wildcards
						EntityAttribute depthLevelAttribute = searchBE.findEntityAttribute("SCH_WILDCARD_DEPTH")
								.orElse(null);
						Integer depth = 0;
						if (depthLevelAttribute != null) {
							depth = depthLevelAttribute.getValueInteger();
						}

						/*
						 * NOTE: We must build the wildcard where condition differently for
						 * whitelists, blacklists and ordinary cases.
						 */

						builder.and(
								baseEntity.name.like(wildcardValue)
										// check code for Dev UI searches
										.or(searchBE.getCode().startsWith("SBE_DEV_UI")
												? baseEntity.code.like(wildcardValue)
												: null)
										.or(!searchBE.getCode().startsWith("SBE_DEV_UI")
												? eaWildcardJoin.valueString.like(wildcardValue)
														.and(
																// build wildcard for whitelist
																wildcardWhiteList.length > 0
																		? eaWildcardJoin.attributeCode
																				.in(wildcardWhiteList)
																		// build wildcard for blacklist
																		: wildcardBlackList.length > 0
																				? eaWildcardJoin.attributeCode
																						.notIn(wildcardBlackList)
																				// nothing for ordinary cases
																				: null)
												: null)
										.or((depth != null && depth > 0)
												? Expressions.stringTemplate("replace({0},'[\"','')",
														Expressions.stringTemplate("replace({0},'\"]','')",
																eaWildcardJoin.valueString))
														.in(generateWildcardSubQuery(wildcardValue, depth,
																wildcardWhiteList, wildcardBlackList))
												: null));

						/*
						 * NOTE: the comments below will be deleted shortly, once confirmed that the
						 * above code functions well.
						 */

						// only wildcard on associations if depth is non zero
						// if (depth != null && depth > 0) {

						// if (wildcardWhiteList.length > 0) {
						// builder.and(baseEntity.name.like(wildcardValue)
						// .or(eaWildcardJoin.valueString.like(wildcardValue).and(eaWildcardJoin.attributeCode.in(wildcardWhiteList)))
						// .or(Expressions.stringTemplate("replace({0},'[\"','')",
						// Expressions.stringTemplate("replace({0},'\"]','')",
						// eaWildcardJoin.valueString)
						// ).in(generateWildcardSubQuery(wildcardValue, depth, wildcardWhiteList,
						// wildcardBlackList))
						// )
						// );

						// } else if (wildcardBlackList.length > 0) {
						// builder.and(baseEntity.name.like(wildcardValue)
						// .or(eaWildcardJoin.valueString.like(wildcardValue).and(eaWildcardJoin.attributeCode.notIn(wildcardBlackList)))
						// .or(Expressions.stringTemplate("replace({0},'[\"','')",
						// Expressions.stringTemplate("replace({0},'\"]','')",
						// eaWildcardJoin.valueString)
						// ).in(generateWildcardSubQuery(wildcardValue, depth, wildcardWhiteList,
						// wildcardBlackList))
						// )
						// );

						// } else {
						// builder.and(baseEntity.name.like(wildcardValue)
						// .or(eaWildcardJoin.valueString.like(wildcardValue))
						// .or(Expressions.stringTemplate("replace({0},'[\"','')",
						// Expressions.stringTemplate("replace({0},'\"]','')",
						// eaWildcardJoin.valueString)
						// ).in(generateWildcardSubQuery(wildcardValue, depth, wildcardWhiteList,
						// wildcardBlackList))
						// )
						// );
						// }

						// } else {

						// builder.and(
						// baseEntity.name.like(wildcardValue)
						// // check code for Dev UI searches
						// .or(searchBE.getCode().equals("SBE_DEV_UI") ?
						// baseEntity.code.like(wildcardValue) : null)
						// .or(eaWildcardJoin.valueString.like(wildcardValue)
						// .and(
						// // build wildcard for whitelist
						// wildcardWhiteList.length > 0 ?
						// eaWildcardJoin.attributeCode.in(wildcardWhiteList)
						// // build wildcard for blacklist
						// : wildcardBlackList.length > 0 ?
						// eaWildcardJoin.attributeCode.notIn(wildcardBlackList)
						// // nothing for ordinary cases
						// : null)
						// )
						// );

						// if (wildcardWhiteList.length > 0) {
						// builder.and(baseEntity.name.like(wildcardValue)
						// .or(eaWildcardJoin.valueString.like(wildcardValue).and(eaWildcardJoin.attributeCode.in(wildcardWhiteList))));

						// // build wildcard for blacklist
						// } else if (wildcardBlackList.length > 0) {
						// builder.and(baseEntity.name.like(wildcardValue)
						// .or(eaWildcardJoin.valueString.like(wildcardValue).and(eaWildcardJoin.attributeCode.notIn(wildcardBlackList))));

						// // build wildcard for ordinary cases
						// } else {
						// builder.and(baseEntity.name.like(wildcardValue)
						// .or(eaWildcardJoin.valueString.like(wildcardValue)));
						// }
						// }
					}
				}
			} else if (attributeCode.startsWith("SCH_LINK_CODE")) {
				linkCode = ea.getValue();
			} else if (attributeCode.startsWith("SCH_LINK_VALUE")) {
				linkValue = ea.getValue();
			} else if (attributeCode.startsWith("SCH_SOURCE_CODE")) {
				sourceCode = ea.getValue();
			} else if (attributeCode.startsWith("SCH_TARGET_CODE")) {
				targetCode = ea.getValue();
			} else if (attributeCode.startsWith("SCH_STATUS")) {
				Integer ordinal = ea.getValueInteger();
				status = EEntityStatus.values()[ordinal];
				log.info("Search Status: [" + status.toString() + ":" + ordinal.toString() + "]");
				// Add to sort list if it is a sort attribute
			} else if (attributeCode.startsWith("SRT_")) {
				sortAttributes.add(ea);
			}
		}
		// Add BaseEntity Status expression
		builder.and(baseEntity.status.loe(status));
		// Order the sorts by weight
		Comparator<EntityAttribute> compareByWeight = (EntityAttribute a, EntityAttribute b) -> a.getWeight()
				.compareTo(b.getWeight());
		Collections.sort(sortAttributes, compareByWeight);
		// Create a Join for each sort
		for (EntityAttribute sort : sortAttributes) {

			String attributeCode = sort.getAttributeCode();
			log.info("Sorting with " + attributeCode);
			QEntityAttribute eaOrderJoin = new QEntityAttribute("eaOrderJoin_" + joinCounter.toString());
			joinCounter++;

			if (!(attributeCode.startsWith("SRT_PRI_CREATED") || attributeCode.startsWith("SRT_PRI_UPDATED")
					|| attributeCode.startsWith("SRT_PRI_CODE") || attributeCode.startsWith("SRT_PRI_NAME"))) {
				query.leftJoin(eaOrderJoin)
						.on(eaOrderJoin.pk.baseEntity.id.eq(baseEntity.id)
								.and(eaOrderJoin.attributeCode.eq(attributeCode)));
			}

			ComparableExpressionBase orderColumn = null;
			if (attributeCode.startsWith("SRT_PRI_CREATED")) {
				// Use ID because there is no index on created, and this gives same result
				orderColumn = baseEntity.id;
			} else if (attributeCode.startsWith("SRT_PRI_UPDATED")) {
				orderColumn = baseEntity.updated;
			} else if (attributeCode.startsWith("SRT_PRI_CODE")) {
				orderColumn = baseEntity.code;
			} else if (attributeCode.startsWith("SRT_PRI_NAME")) {
				orderColumn = baseEntity.name;
			} else {
				// Use Attribute Code to find the datatype, and thus the DB field to sort on
				Attribute attr = qwandaUtils.getAttribute(attributeCode.substring("SRT_".length()));
				String dtt = attr.getDataType().getClassName();
				orderColumn = getPathFromDatatype(dtt, eaOrderJoin);
			}

			if (orderColumn != null) {
				if (sort.getValueString().equals("ASC")) {
					query.orderBy(orderColumn.asc());
				} else {
					query.orderBy(orderColumn.desc());
				}
			} else {
				log.info("orderColumn is null for attribute " + attributeCode);
			}
		}

		// Build link join if necessary
		if (sourceCode != null || targetCode != null || linkCode != null || linkValue != null) {
			QEntityEntity linkJoin = new QEntityEntity("linkJoin");
			BooleanBuilder linkBuilder = new BooleanBuilder();

			log.info("Source Code is " + sourceCode);
			log.info("Target Code is " + targetCode);
			log.info("Link Code is " + linkCode);
			log.info("Link Value is " + linkValue);
			if (sourceCode == null && targetCode == null) {
				// Only look in targetCode if both are null
				linkBuilder.and(linkJoin.link.targetCode.eq(baseEntity.code));
			} else if (sourceCode != null) {
				linkBuilder.and(linkJoin.link.sourceCode.eq(sourceCode));
				if (targetCode == null) {
					linkBuilder.and(linkJoin.link.targetCode.eq(baseEntity.code));
				}
			} else if (targetCode != null) {
				linkBuilder.and(linkJoin.link.targetCode.eq(targetCode));
				if (sourceCode == null) {
					linkBuilder.and(linkJoin.link.sourceCode.eq(baseEntity.code));
				}
			}

			query.join(linkJoin).on(linkBuilder);

			if (linkCode != null) {
				builder.and(linkJoin.link.attributeCode.eq(linkCode));
			}
			if (linkValue != null) {
				builder.and(linkJoin.link.linkValue.eq(linkValue));
			}

			// Order By Weight Of ENTITY_ENTITY link
			if (sortAttributes.size() == 0) {
				query.orderBy(linkJoin.weight.asc());
			}
		}
		// Search across people and companies if from searchbar
		if (searchBE.getCode().startsWith("SBE_SEARCHBAR")) {
			// search across people and companies
			builder.and(baseEntity.code.like("PER_%"))
					.or(baseEntity.code.like("CPY_%"));
		}

		// Add all builder conditions to query
		query.where(builder);
		// Set page start and page size, then fetch codes
		query.offset(pageStart).limit(pageSize);

		Instant middle = Instant.now();
		log.info("Finished BUILDING query with duration: " + Duration.between(start, middle).toMillis()
				+ " millSeconds.");

		if (countOnly) {
			// Fetch only the count
			long count = query.select(baseEntity.code).distinct().fetchCount();
			result = new QSearchBeResult(count);
		} else {
			// Fetch data and count
			if (fetchEntities != null && fetchEntities) {

				List<BaseEntity> entities = query.select(QBaseEntity.baseEntity).distinct().fetch();
				long count = query.fetchCount();

				List<String> allowed = getSearchColumnFilterArray(searchBE);
				BaseEntity[] beArray = new BaseEntity[entities.size()];

				Boolean columnWildcard = searchBE.findEntityAttribute("COL_*").isPresent();

				for (int i = 0; i < entities.size(); i++) {

					BaseEntity be = entities.get(i);

					if (!columnWildcard) {
						be = privacyFilter(be, allowed);
					}
					be = handleSpecialAttributes(be, allowed);

					be.setIndex(i);
					beArray[i] = be;
				}
				result = new QSearchBeResult(beArray, count);

			} else {

				codes = query.select(baseEntity.code).distinct().fetch();
				long count = query.fetchCount();
				result = new QSearchBeResult(codes, count);

			}
		}
		Instant end = Instant.now();
		log.info("Finished RUNNING query with duration: " + Duration.between(middle, end).toMillis() + " millSeconds.");
		// Return codes and count
		log.info("SQL = " + query.toString());
		return result;
	}

	public static Predicate getDateTimePredicate(EntityAttribute ea, DateTimePath path) {
		String condition = SearchEntity.convertFromSaveable(ea.getAttributeName());
		LocalDateTime dateTime = ea.getValueDateTime();

		if (dateTime == null) {
			LocalDate date = ea.getValueDate();
			log.info(ea.getAttributeCode() + " " + condition + " " + date);

			// Convert Date into two DateTime boundaries
			LocalDateTime lowerBound = date.atStartOfDay();
			log.info("lowerBound = " + lowerBound);
			LocalDateTime upperBound = lowerBound.plusDays(1);
			log.info("upperBound = " + upperBound);

			if (condition.equals(">")) {
				return path.after(upperBound);
			} else if (condition.equals(">=")) {
				return path.after(lowerBound);
			} else if (condition.equals("<")) {
				return path.before(lowerBound);
			} else if (condition.equals("<=")) {
				return path.before(upperBound);
			} else if (condition.equals("!=")) {
				return path.notBetween(lowerBound, upperBound);
			} else {
				return path.between(lowerBound, upperBound);
			}
		}
		log.info(ea.getAttributeCode() + " " + condition + " " + dateTime);

		if (condition.equals(">=") || condition.equals(">")) {
			return path.after(dateTime);
		} else if (condition.equals("<=") || condition.equals("<")) {
			return path.before(dateTime);
		} else if (condition.equals("!=")) {
			return path.ne(dateTime);
		}
		// Default to equals
		return path.eq(dateTime);
	}

	/**
	 * return a predicate based on the attribute value and datatype
	 *
	 * @param ea
	 * @param entityAttribute
	 * @return
	 */
	public static Predicate getAttributeSearchColumn(EntityAttribute ea, QEntityAttribute entityAttribute) {

		String attributeFilterValue = ea.getValue().toString();
		String condition = SearchEntity.convertFromSaveable(ea.getAttributeName());
		log.info(ea.getAttributeCode() + " " + condition + " " + attributeFilterValue);

		String valueString = "";
		if (ea.getValueString() != null) {
			valueString = ea.getValueString();
		}
		// Null check on condition and default to equals valuestring
		if (condition == null) {
			log.error("SQL condition is NULL, " + "EntityAttribute baseEntityCode is:" + ea.getBaseEntityCode()
					+ ", attributeCode is: " + ea.getAttributeCode());
			// LIKE
		} else if (condition.equals("LIKE")) {
			return entityAttribute.valueString.like(valueString);
			// NOT LIKE
		} else if (condition.equals("NOT LIKE")) {
			return entityAttribute.valueString.notLike(valueString);
			// EQUALS
		} else if (condition.equals("=")) {
			if (ea.getValueBoolean() != null) {
				return entityAttribute.valueBoolean.eq(ea.getValueBoolean());
			} else if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.eq(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.eq(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.eq(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.eq(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.eq(ea.getValueDateTime());
			} else {
				return entityAttribute.valueString.eq(valueString);
			}
			// NOT EQUALS
		} else if (condition.equals("!=")) {
			if (ea.getValueBoolean() != null) {
				return entityAttribute.valueBoolean.ne(ea.getValueBoolean());
			} else if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.ne(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.ne(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.ne(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.ne(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.ne(ea.getValueDateTime());
			} else {
				return entityAttribute.valueString.ne(valueString);
			}
			// GREATER THAN OR EQUAL TO
		} else if (condition.equals(">=")) {
			if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.goe(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.goe(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.goe(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.goe(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.goe(ea.getValueDateTime());
			}
			// LESS THAN OR EQUAL TO
		} else if (condition.equals("<=")) {
			if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.loe(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.loe(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.loe(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.loe(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.loe(ea.getValueDateTime());
			}
			// GREATER THAN
		} else if (condition.equals(">")) {
			if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.gt(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.gt(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.gt(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.after(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.after(ea.getValueDateTime());
			}
			// LESS THAN
		} else if (condition.equals("<")) {
			if (ea.getValueDouble() != null) {
				return entityAttribute.valueDouble.lt(ea.getValueDouble());
			} else if (ea.getValueInteger() != null) {
				return entityAttribute.valueInteger.lt(ea.getValueInteger());
			} else if (ea.getValueLong() != null) {
				return entityAttribute.valueLong.lt(ea.getValueLong());
			} else if (ea.getValueDate() != null) {
				return entityAttribute.valueDate.before(ea.getValueDate());
			} else if (ea.getValueDateTime() != null) {
				return entityAttribute.valueDateTime.before(ea.getValueDateTime());
			}
		}
		// Default
		return entityAttribute.valueString.eq(valueString);
	}

	/**
	 * Find the value column based off of the attribute datatype
	 *
	 * @param dtt
	 * @param entityAttribute
	 * @return
	 */
	public static ComparableExpressionBase getPathFromDatatype(String dtt, QEntityAttribute entityAttribute) {

		if (dtt.equals("Text")) {
			return entityAttribute.valueString;
		} else if (dtt.equals("java.lang.String") || dtt.equals("String")) {
			return entityAttribute.valueString;
		} else if (dtt.equals("java.lang.Boolean") || dtt.equals("Boolean")) {
			return entityAttribute.valueBoolean;
		} else if (dtt.equals("java.lang.Double") || dtt.equals("Double")) {
			return entityAttribute.valueDouble;
		} else if (dtt.equals("java.lang.Integer") || dtt.equals("Integer")) {
			return entityAttribute.valueInteger;
		} else if (dtt.equals("java.lang.Long") || dtt.equals("Long")) {
			return entityAttribute.valueLong;
		} else if (dtt.equals("java.time.LocalDateTime") || dtt.equals("LocalDateTime")) {
			return entityAttribute.valueDateTime;
		} else if (dtt.equals("java.time.LocalDate") || dtt.equals("LocalDate")) {
			return entityAttribute.valueDate;
		} else if (dtt.equals("java.time.LocalTime") || dtt.equals("LocalTime")) {
			return entityAttribute.valueTime;
		}

		log.warn("Unable to read datatype");
		return entityAttribute.valueString;
	}

	/**
	 * For association filter of format like LNK_PERSON.LNK_COMPANY.PRI_NAME,
	 * this function wil strip the first code in that chain of attributes
	 * whilst retaining any AND/OR prefixs.
	 * 
	 * @param eaList
	 */
	public static void detatchBaseAttributeCode(List<EntityAttribute> eaList) {

		eaList.stream().forEach(ea -> {
			String[] associationArray = ea.getAttributeCode().split("\\.");

			if (associationArray.length > 1) {
				String baseAttributeCode = associationArray[0];

				String prefix = "";
				if (baseAttributeCode.startsWith("AND_")) {
					prefix = "AND_";
				}
				if (baseAttributeCode.startsWith("OR_")) {
					prefix = "OR_";
				}
				// Remove first item and update with prefix at beginning
				String[] newAssociationArray = Arrays.copyOfRange(associationArray, 1, associationArray.length);
				ea.setAttributeCode(prefix + String.join(".", newAssociationArray));
			} else {
				log.warn("Association array length too small. Not updating!");
			}
		});
	}

	/**
	 * Create a sub query for searhing across LNK associations
	 *
	 * This is a recursive function that can run as many
	 * times as is specified by the attribute.
	 *
	 * @param ea The EntityAttribute filter from SBE
	 * @return the subquery object
	 */
	public static JPQLQuery generateSubQuery(List<EntityAttribute> eaList) {

		// Find first attribute that is not AND/OR. There should be only one
		EntityAttribute ea = eaList.stream()
				.filter(x -> (!x.getAttributeCode().startsWith("AND_") && !x.getAttributeCode().startsWith("OR_")))
				.findFirst().get();

		// Random uuid for uniqueness in the query string
		String uuid = UUID.randomUUID().toString().substring(0, 8);

		// Define items to base query upon
		QBaseEntity baseEntity = new QBaseEntity("baseEntity_" + uuid);
		QEntityAttribute entityAttribute = new QEntityAttribute("entityAttribute_" + uuid);

		// Unpack each attributeCode
		String[] associationArray = ea.getAttributeCode().split("\\.");
		String baseAttributeCode = associationArray[0];
		if (associationArray.length > 1) {
			// Prepare for next iteration by removing base code
			detatchBaseAttributeCode(eaList);

			// Recursive search
			return JPAExpressions.selectDistinct(baseEntity.code)
					.from(baseEntity)
					.leftJoin(entityAttribute)
					.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id)
							.and(entityAttribute.attributeCode.eq(baseAttributeCode)))
					.where(
							Expressions.stringTemplate("replace({0},'[\"','')",
									Expressions.stringTemplate("replace({0},'\"]','')", entityAttribute.valueString))
									.in(generateSubQuery(eaList)));
		} else {

			// Create SubQuery Builder parameters using filters
			BooleanBuilder builder = new BooleanBuilder();
			builder.and(getAttributeSearchColumn(ea, entityAttribute));

			// Process AND Filters
			eaList.stream().filter(x -> x.getAttributeCode().startsWith("AND")
					&& removePrefixFromCode(x.getAttributeCode(), "AND").equals(baseAttributeCode))
					.forEach(x -> {
						builder.and(getAttributeSearchColumn(x, entityAttribute));
					});
			// Process OR Filters
			eaList.stream().filter(x -> x.getAttributeCode().startsWith("OR")
					&& removePrefixFromCode(x.getAttributeCode(), "OR").equals(baseAttributeCode))
					.forEach(x -> {
						builder.or(getAttributeSearchColumn(x, entityAttribute));
					});

			// Return the final SubQuery
			return JPAExpressions.selectDistinct(baseEntity.code)
					.from(baseEntity)
					.leftJoin(entityAttribute)
					.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id)
							.and(entityAttribute.attributeCode.eq(baseAttributeCode)))
					.where(builder);
		}
	}

	/**
	 * Generate a sub query to perform a wildcard search on valueString
	 *
	 * @param value
	 * @param recursion
	 * @param whitelist
	 * @param blacklist
	 * @return
	 */
	public static JPQLQuery generateWildcardSubQuery(String value, Integer recursion, String[] whitelist,
			String[] blacklist) {

		// Random uuid to for uniqueness in the query string
		String uuid = UUID.randomUUID().toString().substring(0, 8);

		// Define items to query base upon
		QBaseEntity baseEntity = new QBaseEntity("baseEntity_" + uuid);
		QEntityAttribute entityAttribute = new QEntityAttribute("entityAttribute_" + uuid);

		JPQLQuery exp = JPAExpressions.selectDistinct(baseEntity.code)
				.from(baseEntity)
				.leftJoin(entityAttribute);

		// Handle whitelisting and blacklisting
		if (whitelist.length > 0) {
			exp.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id).and(entityAttribute.attributeCode.in(whitelist)));
		} else if (blacklist.length > 0) {
			exp.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id)
					.and(entityAttribute.attributeCode.notIn(blacklist)));
		} else {
			exp.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id));
		}

		if (recursion > 1) {
			exp.where(entityAttribute.valueString.like(value)
					.or(Expressions.stringTemplate("replace({0},'[\"','')",
							Expressions.stringTemplate("replace({0},'\"]','')", entityAttribute.valueString))
							.in(generateWildcardSubQuery(value, recursion - 1, whitelist, blacklist))));
		} else {
			exp.where(entityAttribute.valueString.like(value));
		}
		return exp;
	}

	/**
	 * Quick tool to remove any prefix strings from attribute codes, even if the
	 * prefix occurs multiple times.
	 * 
	 * @param code   The attribute code
	 * @param prefix The prefix to remove
	 * @return formatted The formatted code
	 */
	public static String removePrefixFromCode(String code, String prefix) {

		String formatted = code;
		while (formatted.startsWith(prefix + "_")) {
			formatted = formatted.substring(prefix.length() + 1);
		}
		return formatted;
	}

	public static List<String> getSearchColumnFilterArray(SearchEntity searchBE) {
		List<String> attributeFilter = new ArrayList<String>();
		List<String> assocAttributeFilter = new ArrayList<String>();

		for (EntityAttribute ea : searchBE.getBaseEntityAttributes()) {
			String attributeCode = ea.getAttributeCode();
			if (attributeCode.startsWith("COL_") || attributeCode.startsWith("CAL_")) {
				if (attributeCode.equals("COL_PRI_ADDRESS_FULL")) {
					attributeFilter.add("PRI_ADDRESS_LATITUDE");
					attributeFilter.add("PRI_ADDRESS_LONGITUDE");
				}
				if (attributeCode.startsWith("COL__")) {
					String[] splitCode = attributeCode.substring("COL__".length()).split("__");
					assocAttributeFilter.add(splitCode[0]);
				} else {
					attributeFilter.add(attributeCode.substring("COL_".length()));
				}
			}
		}
		attributeFilter.addAll(assocAttributeFilter);
		return attributeFilter;
	}

	public static BaseEntity privacyFilter(BaseEntity be, List<String> allowed) {

		// Filter out unwanted attributes
		be.setBaseEntityAttributes(
				be.getBaseEntityAttributes()
						.stream()
						.filter(x -> allowed.contains(x.getAttributeCode()))
						.collect(Collectors.toSet()));

		return be;
	}

	public static BaseEntity handleSpecialAttributes(BaseEntity be, List<String> allowed) {

		try {
			// Handle Created and Updated attributes
			if (allowed.contains("PRI_CREATED")) {
				Attribute createdAttr = new Attribute("PRI_CREATED", "Created", new DataType(LocalDateTime.class));
				EntityAttribute created = new EntityAttribute(be, createdAttr, 1.0);
				created.setValueDateTime(be.getCreated());
				be.addAttribute(created);
			}
			if (allowed.contains("PRI_CREATED_DATE")) {
				Attribute createdAttr = new Attribute("PRI_CREATED_DATE", "Created", new DataType(LocalDate.class));
				EntityAttribute created = new EntityAttribute(be, createdAttr, 1.0);
				created.setValueDate(be.getCreated().toLocalDate());
				be.addAttribute(created);
			}
			if (allowed.contains("PRI_UPDATED")) {
				Attribute updatedAttr = new Attribute("PRI_UPDATED", "Updated", new DataType(LocalDateTime.class));
				EntityAttribute updated = new EntityAttribute(be, updatedAttr, 1.0);
				updated.setValueDateTime(be.getUpdated());
				be.addAttribute(updated);
			}
			if (allowed.contains("PRI_UPDATED_DATE")) {
				Attribute updatedAttr = new Attribute("PRI_UPDATED_DATE", "Updated", new DataType(LocalDate.class));
				EntityAttribute updated = new EntityAttribute(be, updatedAttr, 1.0);
				updated.setValueDate(be.getUpdated().toLocalDate());
				be.addAttribute(updated);
			}
			if (allowed.contains("PRI_CODE")) {
				Attribute codeAttr = new Attribute("PRI_CODE", "Code", new DataType(String.class));
				EntityAttribute code = new EntityAttribute(be, codeAttr, 1.0);
				code.setValueString(be.getCode());
				be.addAttribute(code);
			}
		} catch (BadDataException e) {
			log.error("could not add special attributes to entity");
		}

		return be;
	}

	public Answer getAssociatedColumnValue(BaseEntity baseBE, String calEACode) {

		String[] calFields = calEACode.substring("COL__".length()).split("__");
		if (calFields.length == 1) {
			log.error("CALS length is bad for :" + calEACode);
			return null;
		}
		String linkBeCode = calFields[calFields.length - 1];

		BaseEntity be = baseBE;

		Optional<EntityAttribute> associateEa = null;
		// log.info("calFields value " + calEACode);
		// log.info("linkBeCode value " + linkBeCode);

		String finalAttributeCode = calEACode.substring("COL_".length());
		// Fetch The Attribute of the last code
		String primaryAttrCode = calFields[calFields.length - 1];
		Attribute primaryAttribute = qwandaUtils.getAttribute(primaryAttrCode);

		Answer ans = new Answer(baseBE.getCode(), baseBE.getCode(), finalAttributeCode, "");
		Attribute att = new Attribute(finalAttributeCode, primaryAttribute.getName(), primaryAttribute.getDataType());
		ans.setAttribute(att);

		for (int i = 0; i < calFields.length - 1; i++) {

			String attributeCode = calFields[i];
			String calBe = be.getValueAsString(attributeCode);

			if (calBe != null && !StringUtils.isBlank(calBe)) {

				String calVal = calBe.replace("\"", "").replace("[", "").replace("]", "").replace(" ", "");
				String[] codeArr = calVal.split(",");

				for (String code : codeArr) {
					if (StringUtils.isBlank(code)) {
						log.error("code from Calfields is empty calVal[" + calVal + "] skipping calFields=["
								+ calFields.toString() + "] - be:" + baseBE.getCode());
						continue;
					}

					BaseEntity associatedBe = beUtils.getBaseEntityByCode(code);
					if (associatedBe == null) {
						log.debug("associatedBe DOES NOT exist ->" + code);
						return null;
					}

					if (i == (calFields.length - 2)) {
						associateEa = associatedBe.findEntityAttribute(linkBeCode);

						if (associateEa != null && (associateEa.isPresent() || ("PRI_NAME".equals(linkBeCode)))) {
							String linkedValue = null;
							if ("PRI_NAME".equals(linkBeCode)) {
								linkedValue = associatedBe.getName();
							} else {
								linkedValue = associatedBe.getValueAsString(linkBeCode);
							}
							if (!ans.getValue().isEmpty()) {
								linkedValue = ans.getValue() + "," + linkedValue;
							}
							ans.setValue(linkedValue);
						} else {
							log.debug("No attribute present fo CAL EA");
						}
					}
					be = associatedBe;
				}
			} else {
				log.debug("Could not find attribute value for " + attributeCode + " for entity " + be.getCode());
				return null;
			}
		}

		return ans;
	}

}
