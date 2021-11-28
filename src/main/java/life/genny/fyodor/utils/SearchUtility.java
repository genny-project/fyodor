package life.genny.fyodor.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.Comparator;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.time.*;
import java.util.UUID;
import java.util.Arrays;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.querydsl.core.types.dsl.DateTimePath;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Predicate;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.JPQLQuery;

import life.genny.fyodor.models.GennyToken;
import life.genny.qwandaq.attribute.Attribute;
import life.genny.qwandaq.attribute.EntityAttribute;
import life.genny.qwandaq.entity.SearchEntity;
import life.genny.qwandaq.message.QDataBaseEntityMessage;
import life.genny.qwandaq.attribute.QEntityAttribute;
import life.genny.qwandaq.datatype.DataType;
import life.genny.qwandaq.entity.BaseEntity;
import life.genny.qwandaq.entity.QBaseEntity;
import life.genny.qwandaq.EEntityStatus;
import life.genny.qwandaq.entity.QEntityEntity;
import life.genny.qwandaq.message.QSearchBeResult;
import life.genny.fyodor.service.ApiService;

public class SearchUtility {

	private static final Logger log = Logger.getLogger(SearchUtility.class);

	@Inject
	@RestClient
	ApiService apiService;

	EntityManager entityManager;

	GennyToken serviceToken;

	Jsonb jsonb = JsonbBuilder.create();

    static public Map<String,Map<String, Attribute>> realmAttributeMap = new ConcurrentHashMap<>();

	public SearchUtility(GennyToken serviceToken, EntityManager entityManager) {
		this.serviceToken = serviceToken;
		this.entityManager = entityManager;
	}

	public void processSearchEvent(String data) {

		// QDataBaseEntityMessage msg = JsonUtils.fromJson(data, QDataBaseEntityMessage.class);
		QDataBaseEntityMessage msg = new QDataBaseEntityMessage();

		SearchEntity searchBE = (SearchEntity) msg.getItems()[0];

		QSearchBeResult results = findBySearch25(serviceToken.getRealm(), searchBE, false, true);
	}

	/**
	 * Perform a safe search using named parameters to
	 * protect from SQL Injection
	*/
	public QSearchBeResult findBySearch25(String realm, final SearchEntity searchBE, Boolean countOnly, Boolean fetchEntities) {

		Integer defaultPageSize = 20;
		// Init necessary vars
		QSearchBeResult result = null;
		List<String> codes = new ArrayList<String>();
		// Get page start and page size from SBE
		Integer pageStart = searchBE.getPageStart(0);
		Integer pageSize = searchBE.getPageSize(defaultPageSize);
		// Integer pageSize = searchBE.getPageSize(defaultPageSize);

		JPAQuery<?> query = new JPAQuery<Void>(entityManager);

		QBaseEntity baseEntity = new QBaseEntity("baseEntity");
		// Define a join for link searches
		String linkCode = null;
		String linkValue = null;
		String sourceCode = null;
		String targetCode = null;

		query.from(baseEntity);

		List<EntityAttribute> sortAttributes = new ArrayList<>();

		// Find AND and OR attributes and remove these prefixs from each of them
		List<EntityAttribute> andAttributes = searchBE.findPrefixEntityAttributes("AND_");
		List<EntityAttribute> orAttributes = searchBE.findPrefixEntityAttributes("OR_");

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
				andAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode)).forEach(x -> {
					log.info("AND " + attributeCode + " like " + x.getAsString());
					entityCodeBuilder.and(baseEntity.code.like(x.getAsString()));
				});
				orAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode)).forEach(x -> {
					log.info("OR " + attributeCode + " like " + x.getAsString());
					entityCodeBuilder.or(baseEntity.code.like(x.getAsString()));
				});
				builder.and(entityCodeBuilder);

			// Handle Created Date Filters
			} else if (attributeCode.startsWith("PRI_CREATED")) {
				builder.and(getDateTimePredicate(ea, baseEntity.created));
				// Process any AND/OR filters for this attribute
				andAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode)).forEach(x -> {
					builder.and(getDateTimePredicate(x, baseEntity.created));
				});
				orAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode)).forEach(x -> {
					builder.or(getDateTimePredicate(x, baseEntity.created));
				});

			// Handle Updated Date Filters
			} else if (attributeCode.startsWith("PRI_UPDATED")) {
				builder.and(getDateTimePredicate(ea, baseEntity.updated));
				// Process any AND/OR filters for this attribute
				andAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode)).forEach(x -> {
					builder.and(getDateTimePredicate(x, baseEntity.updated));
				});
				orAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode)).forEach(x -> {
					builder.or(getDateTimePredicate(x, baseEntity.updated));
				});

			// Create a Join for each attribute filters
			} else if (
					(attributeCode.startsWith("PRI_") || attributeCode.startsWith("LNK_"))
					&& !attributeCode.equals("PRI_CODE") && !attributeCode.equals("PRI_TOTAL_RESULTS")
					&& !attributeCode.startsWith("PRI_CREATED") && !attributeCode.startsWith("PRI_UPDATED")
					&& !attributeCode.equals("PRI_INDEX")
				) {

				// Generally don't accept filter LIKE "%", unless other filters present for this attribute
				Boolean isAnyStringFilter = false;
				try {
					if (ea.getValueString() != null && "%".equals(ea.getValueString()) && "LIKE".equals(ea.getAttributeName())) {
						isAnyStringFilter = true;
					}
				} catch (Exception e) {
					log.error("Bad Null ["+ea+"]"+e.getLocalizedMessage());
				}

				String filterName = "eaFilterJoin_"+joinCounter.toString();
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
					List<EntityAttribute> subQueryEaList = searchBE.getBaseEntityAttributes().stream().filter(x -> (
							removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode)
							|| removePrefixFromCode(x.getAttributeCode(), "OR").equals(attributeCode)
							)).collect(Collectors.toList());

					// Prepare for subquery by removing base attribute codes
					detatchBaseAttributeCode(subQueryEaList);
					// Must strip value to get clean code
					currentAttributeBuilder.and(
							Expressions.stringTemplate("replace({0},'[\"','')", 
								Expressions.stringTemplate("replace({0},'\"]','')", eaFilterJoin.valueString)
							) .in(generateSubQuery(subQueryEaList)));
				} else {
					currentAttributeBuilder.and(getAttributeSearchColumn(ea, eaFilterJoin));

					// Process any AND/OR filters for this attribute
					andAttributes.stream().filter(x -> removePrefixFromCode(x.getAttributeCode(), "AND").equals(attributeCode)).forEach(x -> {
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
						// wildcardValue = wildcardValue.replaceAll("[^A-zA-Z0-9 .,'/@()_-]", "");
						wildcardValue = "%" + wildcardValue + "%";

						QEntityAttribute eaWildcardJoin = new QEntityAttribute("eaWildcardJoin");
						query.leftJoin(eaWildcardJoin).on(eaWildcardJoin.pk.baseEntity.id.eq(baseEntity.id));

						builder.and(eaWildcardJoin.valueString.like(wildcardValue).or(baseEntity.name.like(wildcardValue)));
						log.info("WILDCARD like " + wildcardValue);

						builder.and(baseEntity.name.like(wildcardValue)
								.or(eaWildcardJoin.valueString.like(wildcardValue))
								.or(Expressions.stringTemplate("replace({0},'[\"','')", 
										Expressions.stringTemplate("replace({0},'\"]','')", eaWildcardJoin.valueString)
										).in(generateWildcardSubQuery(wildcardValue, 1))
									));
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
			// Add to sort list if it is a sort attribute
			} else if (attributeCode.startsWith("SRT_")) {
				sortAttributes.add(ea);
			}
		}
		// Add BaseEntity Status expression
		// builder.and(baseEntity.status.coalesce(EEntityStatus.ACTIVE).getValue().loe(status));
		builder.and(baseEntity.status.loe(status));
		// Order the sorts by weight
		Comparator<EntityAttribute> compareByWeight = (EntityAttribute a, EntityAttribute b) -> a.getWeight().compareTo(b.getWeight());
		Collections.sort(sortAttributes, compareByWeight);
		// Create a Join for each sort
		for (EntityAttribute sort : sortAttributes) {

			String attributeCode = sort.getAttributeCode();
			log.info("Sorting with " + attributeCode);
			QEntityAttribute eaOrderJoin = new QEntityAttribute("eaOrderJoin_"+joinCounter.toString());
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
				Attribute attr = getAttribute(attributeCode.substring("SRT_".length()), serviceToken);
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

		Instant start = Instant.now();
		String debugStr = "Search25DEBUG";
		log.info(debugStr + " Start building link join");
		// Build link join if necessary
		if (sourceCode != null || targetCode != null || linkCode != null || linkValue != null) {
			QEntityEntity linkJoin = new QEntityEntity("linkJoin");
			BooleanBuilder linkBuilder = new BooleanBuilder();

			log.info("Source Code is "+sourceCode);
			log.info("Target Code is "+targetCode);
			log.info("Link Code is "+linkCode);
			log.info("Link Value is "+linkValue);
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

		Instant end = Instant.now();
		Duration timeElapsed = Duration.between(start, end);
		log.info(debugStr + " Finished building link join, cost:" + timeElapsed.toMillis() + " millSeconds.");

		start = Instant.now();
		log.info(debugStr + " Start query, countOnly=" + countOnly);
		if (countOnly) {
			// Fetch only the count
			long count = query.select(baseEntity.code).distinct().fetchCount();
			result = new QSearchBeResult(count);
		} else {
			// Fetch codes and count
			codes = query.select(baseEntity.code).distinct().fetch();
			long count = query.fetchCount();
			result = new QSearchBeResult(codes, count);

			if (fetchEntities != null && fetchEntities) {

				String[] filterArray = getSearchColumnFilterArray(searchBE).toArray(new String[0]);
				BaseEntity[] beArray = new BaseEntity[codes.size()];

				for (int i = 0; i < codes.size(); i++) {

					String code = codes.get(i);
					BaseEntity be = fetchBaseEntityFromCache(code, serviceToken);
					be = privacyFilter(be, filterArray);
					be.setIndex(i);
					beArray[i] = be;
				}

				result.setEntities(beArray);
			}
		}
		end = Instant.now();
		log.info(debugStr + " Finished building link join, cost:" + Duration.between(start, end).toMillis() + " millSeconds.");
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
	
	public static Predicate getAttributeSearchColumn(EntityAttribute ea, QEntityAttribute entityAttribute) {

		// TODO: Make this function more neat, and less repetitive - Jasper (19/08/2021)

		String attributeFilterValue = ea.getValue().toString();
		String condition = SearchEntity.convertFromSaveable(ea.getAttributeName());
		System.out.println(ea.getAttributeCode() + " " + condition + " " + attributeFilterValue);

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
		EntityAttribute ea = eaList.stream().filter(x -> (!x.getAttributeCode().startsWith("AND_") && !x.getAttributeCode().startsWith("OR_"))).findFirst().get();

		// Random uuid for uniqueness in the query string
		String uuid = UUID.randomUUID().toString().substring(0, 8);

		// Define items to base query upon
		QBaseEntity baseEntity = new QBaseEntity("baseEntity_"+uuid);
		QEntityAttribute entityAttribute = new QEntityAttribute("entityAttribute_"+uuid);

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
						Expressions.stringTemplate("replace({0},'\"]','')", entityAttribute.valueString)
					).in(generateSubQuery(eaList)));
		} else {

			// Create SubQuery Builder parameters using filters
			BooleanBuilder builder = new BooleanBuilder();
			builder.and(getAttributeSearchColumn(ea, entityAttribute));

			// Process AND Filters
			eaList.stream().filter(x -> 
					x.getAttributeCode().startsWith("AND")
					&& removePrefixFromCode(x.getAttributeCode(), "AND").equals(baseAttributeCode))
				.forEach(x -> {
					builder.and(getAttributeSearchColumn(x, entityAttribute));
			});
			// Process OR Filters
			eaList.stream().filter(x -> 
					x.getAttributeCode().startsWith("OR")
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

	public static JPQLQuery generateWildcardSubQuery(String value, Integer recursion) {

		// Random uuid to for uniqueness in the query string
		String uuid = UUID.randomUUID().toString().substring(0, 8);

		// Define items to query base upon
		QBaseEntity baseEntity = new QBaseEntity("baseEntity_"+uuid);
		QEntityAttribute entityAttribute = new QEntityAttribute("entityAttribute_"+uuid);


		if (recursion > 1) {
			return JPAExpressions.selectDistinct(baseEntity.code)
				.from(baseEntity)
				.leftJoin(entityAttribute)
				.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id))
				.where(entityAttribute.valueString.like(value)
						.or(
							Expressions.stringTemplate("replace({0},'[\"','')", 
								Expressions.stringTemplate("replace({0},'\"]','')", entityAttribute.valueString)
								).in(generateWildcardSubQuery(value, recursion-1))
							));

		} else {
			return JPAExpressions.selectDistinct(baseEntity.code)
				.from(baseEntity)
				.leftJoin(entityAttribute)
				.on(entityAttribute.pk.baseEntity.id.eq(baseEntity.id))
				.where(entityAttribute.valueString.like(value));
		}
	}

	public static List<String> getSearchColumnFilterArray(SearchEntity searchBE)
	{
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

    public static BaseEntity privacyFilter(BaseEntity be, final String[] filterAttributes) {
        Set<EntityAttribute> allowedAttributes = new HashSet<EntityAttribute>();
        for (EntityAttribute entityAttribute : be.getBaseEntityAttributes()) {
            String attributeCode = entityAttribute.getAttributeCode();
            if (Arrays.stream(filterAttributes).anyMatch(x -> x.equals(attributeCode))) {
                allowedAttributes.add(entityAttribute);
            }
        }
        // Handle Created and Updated attributes
        if (Arrays.asList(filterAttributes).contains("PRI_CREATED")) {
            // log.info("filterAttributes contains PRI_CREATED");
            Attribute createdAttr = new Attribute("PRI_CREATED", "Created", new DataType(LocalDateTime.class));
            EntityAttribute created = new EntityAttribute(be, createdAttr, 1.0);
            created.setValueDateTime(be.getCreated());
            allowedAttributes.add(created);// allow attributes that starts with "LNK_"
        }
        if (Arrays.asList(filterAttributes).contains("PRI_CREATED_DATE")) {
            // log.info("filterAttributes contains PRI_CREATED_DATE");
            Attribute createdAttr = new Attribute("PRI_CREATED_DATE", "Created", new DataType(LocalDate.class));
            EntityAttribute created = new EntityAttribute(be, createdAttr, 1.0);
            created.setValueDate(be.getCreated().toLocalDate());
            allowedAttributes.add(created);// allow attributes that starts with "LNK_"
        }
        if (Arrays.asList(filterAttributes).contains("PRI_UPDATED")) {
            // log.info("filterAttributes contains PRI_UPDATED");
            Attribute updatedAttr = new Attribute("PRI_UPDATED", "Updated", new DataType(LocalDateTime.class));
            EntityAttribute updated = new EntityAttribute(be, updatedAttr, 1.0);
            updated.setValueDateTime(be.getUpdated());
            allowedAttributes.add(updated);// allow attributes that starts with "LNK_"
        }
        if (Arrays.asList(filterAttributes).contains("PRI_UPDATED_DATE")) {
            // log.info("filterAttributes contains PRI_UPDATED_DATE");
            Attribute updatedAttr = new Attribute("PRI_UPDATED_DATE", "Updated", new DataType(LocalDate.class));
            EntityAttribute updated = new EntityAttribute(be, updatedAttr, 1.0);
            updated.setValueDate(be.getUpdated().toLocalDate());
            allowedAttributes.add(updated);// allow attributes that starts with "LNK_"
        }
        be.setBaseEntityAttributes(allowedAttributes);

        return be;
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

	public BaseEntity fetchBaseEntityFromCache(final String code, final GennyToken token) {
		String data = null;
		String value = null;

		data = apiService.getDataFromCache(token.getRealm(), code, "Bearer " + token.getToken());
		JsonObject json = jsonb.fromJson(data, JsonObject.class);
		if ("ok".equalsIgnoreCase(json.getString("status"))) {
			value = json.getString("value");
			// log.info(value);
			return jsonb.fromJson(value, BaseEntity.class);
		}
		return null;
	}

    public Attribute getAttribute(final String attributeCode, final GennyToken gennyToken) {

    	String realm = gennyToken.getRealm();

    	if (realmAttributeMap.get(gennyToken.getRealm())==null) {
    		loadAllAttributesIntoCache(gennyToken);
    	}

        Attribute attribute = realmAttributeMap.get(realm).get(attributeCode);

		if (attribute == null) {
			log.error("Bad Attribute in Map for realm " +realm + " and code " + attributeCode);
		}

        return attribute;
    }

    public void loadAllAttributesIntoCache(final GennyToken gennyToken) {

		String realm = gennyToken.getRealm();

		log.info("About to load all attributes for realm " + realm);

		List<Attribute> attributeList = findAttributes(gennyToken);

		if (attributeList == null) {
			log.error("Null attributeList, not putting in map!!!");
			return;
		}

		if (!realmAttributeMap.containsKey(realm)) {
			realmAttributeMap.put(realm, new ConcurrentHashMap<String,Attribute>());
		}
		Map<String,Attribute> attributeMap = realmAttributeMap.get(realm);

		for (Attribute attribute : attributeList) {
			attributeMap.put(attribute.getCode(), attribute);
		}

		// realmAttributeMap.put(realm, attributeMap);

		log.info("All attributes have been loaded in from DB: " + attributeMap.size() + " attributes loaded!");

    }

	@Transactional
	public List<Attribute> findAttributes(GennyToken gennyToken) throws NoResultException {

        try {

			final List<Attribute> results = entityManager.createQuery("SELECT a FROM Attribute a where a.realm=:realmStr and a.name not like 'App\\_%'")
					.setParameter("realmStr", gennyToken.getRealm())
					.getResultList();

			return results;

        } catch (NoResultException e) {
            log.error("No results found from DB search");
            e.printStackTrace();
		}
		return null;
	}

}
