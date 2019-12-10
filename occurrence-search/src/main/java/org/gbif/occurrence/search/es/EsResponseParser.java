package org.gbif.occurrence.search.es;

import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.OccurrenceRelation;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.*;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.occurrence.common.TermUtils;

import java.net.URI;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Maps;

import org.elasticsearch.common.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.es.OccurrenceEsField.RELATION;
import static org.gbif.occurrence.search.es.OccurrenceEsField.*;

public class EsResponseParser {

  private static final Pattern NESTED_PATTERN = Pattern.compile("^\\w+(\\.\\w+)+$");
  private static final Predicate<String> IS_NESTED = s -> NESTED_PATTERN.matcher(s).find();
  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private static final Logger LOG = LoggerFactory.getLogger(EsResponseParser.class);

  /**
   * Private constructor.
   */
  private EsResponseParser() {
    //DO NOTHING
  }

  /**
   * Builds a SearchResponse instance using the current builder state.
   *
   * @return a new instance of a SearchResponse.
   */
  public static SearchResponse<Occurrence, OccurrenceSearchParameter> buildDownloadResponse(
      org.elasticsearch.action.search.SearchResponse esResponse, OccurrenceSearchRequest request) {

    SearchResponse<Occurrence, OccurrenceSearchParameter> response = new SearchResponse<>(request);
    response.setCount(esResponse.getHits().getTotalHits());
    parseHits(esResponse, true).ifPresent(response::setResults);
    parseFacets(esResponse, request).ifPresent(response::setFacets);

    return response;
  }

  /**
   * Builds a SearchResponse instance using the current builder state.
   * This response is intended to be used for occurrence downloads only since it does not exclude verbatim fields.
   *
   * @return a new instance of a SearchResponse.
   */
  public static SearchResponse<Occurrence, OccurrenceSearchParameter> buildDownloadResponse(
      org.elasticsearch.action.search.SearchResponse esResponse, Pageable request) {

    SearchResponse<Occurrence, OccurrenceSearchParameter> response = new SearchResponse<>(request);
    response.setCount(esResponse.getHits().getTotalHits());
    parseHits(esResponse, false).ifPresent(response::setResults);
    return response;
  }

  public static List<String> buildSuggestResponse(org.elasticsearch.action.search.SearchResponse esResponse,
                                                  OccurrenceSearchParameter parameter) {

    String fieldName = SEARCH_TO_ES_MAPPING.get(parameter).getFieldName();

    return esResponse.getSuggest().getSuggestion(fieldName).getEntries().stream()
            .flatMap(e -> ((CompletionSuggestion.Entry) e).getOptions().stream())
            .map(CompletionSuggestion.Entry.Option::getHit)
            .map(hit -> hit.getSourceAsMap().get(fieldName))
            .filter(Objects::nonNull)
            .map(String::valueOf)
            .collect(Collectors.toList());
  }

  /**
   * Extract the buckets of an {@link Aggregation}.
   */
  private static List<? extends Terms.Bucket> getBuckets(Aggregation aggregation) {
    if (aggregation instanceof Terms) {
      return ((Terms) aggregation).getBuckets();
    } else if (aggregation instanceof Filter) {
      return
        ((Filter) aggregation)
          .getAggregations().asList()
            .stream()
            .flatMap(agg -> ((Terms) agg).getBuckets().stream())
            .collect(Collectors.toList());
    } else {
      throw new IllegalArgumentException(aggregation.getClass() + " aggregation not supported");
    }
  }

  private static Optional<List<Facet<OccurrenceSearchParameter>>> parseFacets(
      org.elasticsearch.action.search.SearchResponse esResponse, OccurrenceSearchRequest request) {
    return Optional.ofNullable(esResponse.getAggregations())
        .map(
            aggregations ->
                aggregations.asList().stream()
                    .map(
                        aggs -> {
                          // get buckets
                          List<? extends Terms.Bucket> buckets = getBuckets(aggs);

                          // get facet of the agg
                          OccurrenceSearchParameter facet =
                              ES_TO_SEARCH_MAPPING.get(aggs.getName());

                          // check for paging in facets
                          long facetOffset = extractFacetOffset(request, facet);
                          long facetLimit = extractFacetLimit(request, facet);

                          List<Facet.Count> counts =
                              buckets.stream()
                                  .skip(facetOffset)
                                  .limit(facetOffset + facetLimit)
                                  .map(
                                      b ->
                                          new Facet.Count(
                                              b.getKeyAsString(),
                                              b.getDocCount()))
                                  .collect(Collectors.toList());

                          return new Facet<>(facet, counts);
                        })
                    .collect(Collectors.toList()));
  }

  private static Optional<List<Occurrence>> parseHits(org.elasticsearch.action.search.SearchResponse esResponse, boolean excludeInterpreted) {
    if (esResponse.getHits() == null || esResponse.getHits().getHits() == null || esResponse.getHits().getHits().length == 0) {
      return Optional.empty();
    }

    return Optional.of(Stream.of(esResponse.getHits().getHits())
                         .map(hit -> EsResponseParser.toOccurrence(hit, excludeInterpreted))
                         .collect(Collectors.toList()));
  }

  /**
   * Transforms a SearchHit into a suitable Verbatim map of terms.
   */
  public static VerbatimOccurrence toVerbatimOccurrence(SearchHit hit) {
    VerbatimOccurrence vOcc = new VerbatimOccurrence();
    getValue(hit, PUBLISHING_COUNTRY, v -> Country.fromIsoCode(v.toUpperCase()))
      .ifPresent(vOcc::setPublishingCountry);
    getValue(hit, DATASET_KEY, UUID::fromString).ifPresent(vOcc::setDatasetKey);
    getValue(hit, INSTALLATION_KEY, UUID::fromString).ifPresent(vOcc::setInstallationKey);
    getValue(hit, PUBLISHING_ORGANIZATION_KEY, UUID::fromString)
      .ifPresent(vOcc::setPublishingOrgKey);
    getValue(hit, PROTOCOL, EndpointType::fromString).ifPresent(vOcc::setProtocol);

    getListValue(hit, NETWORK_KEY)
      .ifPresent(
        v -> vOcc.setNetworkKeys(v.stream().map(UUID::fromString).collect(Collectors.toList())));
    getValue(hit, CRAWL_ID, Integer::valueOf).ifPresent(vOcc::setCrawlId);
    getDateValue(hit, LAST_PARSED).ifPresent(vOcc::setLastParsed);
    getDateValue(hit, LAST_CRAWLED).ifPresent(vOcc::setLastCrawled);
    getValue(hit, GBIF_ID, Long::valueOf)
      .ifPresent(
        id -> {
          vOcc.setKey(id);
          vOcc.getVerbatimFields().put(GbifTerm.gbifID, String.valueOf(id));
        });
    // add verbatim fields
    Map<String, Object> verbatimData = (Map<String, Object>) hit.getSourceAsMap().get("verbatim");

    vOcc.getVerbatimFields().putAll(parseVerbatimTermMap((Map<String, Object>)(verbatimData).get("core")));
    setIdentifier(hit, vOcc);

    if (verbatimData.containsKey("extensions" )) {
      vOcc.setExtensions(parseExtensionsMap((Map<String, Object>)verbatimData.get("extensions")));
    }

    return vOcc;
  }

  private static Map<Extension, List<Map<Term, String>>> parseExtensionsMap(Map<String, Object> extensions) {
    // parse extensions
    Map<Extension, List<Map<Term, String>>> extTerms = Maps.newHashMap();
    for (String rowType : extensions.keySet()) {
      // first pare into a term cause the extension lookup by rowType is very strict
      Term rowTypeTerm = TERM_FACTORY.findTerm(rowType);
      Extension ext = Extension.fromRowType(rowTypeTerm.qualifiedName());
      if (ext == null) {
        LOG.debug("Ignore unknown extension {}", rowType);
      } else {
        List<Map<Term, String>> records = new ArrayList<>();
        // transform records to term based map
        for (Map<String, Object> rawRecord : (List<Map<String, Object>>) extensions.get(rowType)) {
          records.add(parseVerbatimTermMap(rawRecord));
        }
        extTerms.put(ext, records);
      }
    }
    return extTerms;
  }

  /**
   * Parses a simple string based map into a Term based map, ignoring any non term entries and not parsing nested
   * e.g. extensions data.
   * This produces a Map of verbatim data.
   */
  private static Map<Term, String> parseVerbatimTermMap(Map<String, Object> data) {

    Map<Term, String> terms = Maps.newHashMap();
    data.forEach( (simpleTermName,value) -> {
      if (Objects.nonNull(value) && !simpleTermName.equalsIgnoreCase("extensions")) {
        Term term = TERM_FACTORY.findTerm(simpleTermName);
        terms.put(term, value.toString());
      }
    });

    return terms;
  }

  public static Occurrence toOccurrence(SearchHit hit, boolean excludeInterpreted) {
    // create occurrence
    Occurrence occ = new Occurrence();

    // set fields
    setOccurrenceFields(hit, occ);
    setLocationFields(hit, occ);
    setTemporalFields(hit, occ);
    setCrawlingFields(hit, occ);
    setDatasetFields(hit, occ);
    setTaxonFields(hit, occ);

    // issues
    getListValue(hit, ISSUE)
        .ifPresent(
            v ->
                occ.setIssues(
                    v.stream().map(issue -> VocabularyUtils.lookup(issue, OccurrenceIssue.class))
                      .filter(com.google.common.base.Optional::isPresent)
                      .map(com.google.common.base.Optional::get)
                      .collect(Collectors.toSet())));

    // multimedia extension
    parseMultimediaItems(hit, occ);

    // add verbatim fields
    occ.getVerbatimFields().putAll(extractVerbatimFields(hit, excludeInterpreted));
    // TODO: add verbatim extensions

    setIdentifier(hit, occ);

    return occ;
  }

  /**
   * The id (the <id> reference in the DWCA meta.xml) is an identifier local to the DWCA, and could only have been
   * used for "un-starring" a DWCA star record. However, we've exposed it as DcTerm.identifier for a long time in
   * our public API v1, so we continue to do this.
   */
  private static void setIdentifier(SearchHit hit, VerbatimOccurrence occ) {

    String institutionCode = occ.getVerbatimField(DwcTerm.institutionCode);
    String collectionCode = occ.getVerbatimField(DwcTerm.collectionCode);
    String catalogNumber = occ.getVerbatimField(DwcTerm.catalogNumber);

    // id format following the convention of DwC (http://rs.tdwg.org/dwc/terms/#occurrenceID)
    String triplet = String.join(":", "urn:catalog", institutionCode, collectionCode, catalogNumber);

    String gbifId = Optional.ofNullable(occ.getKey()).map(x -> Long.toString(x)).orElse("");
    String occId = occ.getVerbatimField(DwcTerm.occurrenceID);

    getStringValue(hit, ID)
      .filter(k -> !k.equals(gbifId) && (!Strings.isNullOrEmpty(occId) || !k.equals(triplet)))
      .ifPresent(result -> occ.getVerbatimFields().put(DcTerm.identifier, result));
  }



  private static void setOccurrenceFields(SearchHit hit, Occurrence occ) {
    getValue(hit, GBIF_ID, Long::valueOf)
        .ifPresent(
            id -> {
              occ.setKey(id);
              occ.getVerbatimFields().put(GbifTerm.gbifID, String.valueOf(id));
            });
    getValue(hit, BASIS_OF_RECORD, BasisOfRecord::valueOf).ifPresent(occ::setBasisOfRecord);
    getValue(hit, ESTABLISHMENT_MEANS, EstablishmentMeans::valueOf)
        .ifPresent(occ::setEstablishmentMeans);
    getValue(hit, LIFE_STAGE, LifeStage::valueOf).ifPresent(occ::setLifeStage);
    getDateValue(hit, MODIFIED).ifPresent(occ::setModified);
    getValue(hit, REFERENCES, URI::create).ifPresent(occ::setReferences);
    getValue(hit, SEX, Sex::valueOf).ifPresent(occ::setSex);
    getValue(hit, TYPE_STATUS, TypeStatus::valueOf).ifPresent(occ::setTypeStatus);
    getStringValue(hit, TYPIFIED_NAME).ifPresent(occ::setTypifiedName);
    getValue(hit, INDIVIDUAL_COUNT, Integer::valueOf).ifPresent(occ::setIndividualCount);
    // FIXME: should we have a list of identifiers in the schema?
    getStringValue(hit, IDENTIFIER)
        .ifPresent(
            v -> {
              Identifier identifier = new Identifier();
              identifier.setIdentifier(v);
              occ.setIdentifiers(Collections.singletonList(identifier));
            });

    // FIXME: should we have a list in the schema and all the info of the enum?
    getStringValue(hit, RELATION)
        .ifPresent(
            v -> {
              OccurrenceRelation occRelation = new OccurrenceRelation();
              occRelation.setId(v);
              occ.setRelations(Collections.singletonList(occRelation));
            });
  }

  private static void setTemporalFields(SearchHit hit, Occurrence occ) {
    getDateValue(hit, DATE_IDENTIFIED).ifPresent(occ::setDateIdentified);
    getValue(hit, DAY, Integer::valueOf).ifPresent(occ::setDay);
    getValue(hit, MONTH, Integer::valueOf).ifPresent(occ::setMonth);
    getValue(hit, YEAR, Integer::valueOf).ifPresent(occ::setYear);
    getDateValue(hit, EVENT_DATE).ifPresent(occ::setEventDate);
  }

  private static void setLocationFields(SearchHit hit, Occurrence occ) {
    getValue(hit, CONTINENT, Continent::valueOf).ifPresent(occ::setContinent);
    getStringValue(hit, STATE_PROVINCE).ifPresent(occ::setStateProvince);
    getValue(hit, COUNTRY_CODE, Country::fromIsoCode).ifPresent(occ::setCountry);
    getDoubleValue(hit, COORDINATE_ACCURACY).ifPresent(occ::setCoordinateAccuracy);
    getDoubleValue(hit, COORDINATE_PRECISION).ifPresent(occ::setCoordinatePrecision);
    getDoubleValue(hit, COORDINATE_UNCERTAINTY_METERS)
        .ifPresent(occ::setCoordinateUncertaintyInMeters);
    getDoubleValue(hit, LATITUDE).ifPresent(occ::setDecimalLatitude);
    getDoubleValue(hit, LONGITUDE).ifPresent(occ::setDecimalLongitude);
    getDoubleValue(hit, DEPTH).ifPresent(occ::setDepth);
    getDoubleValue(hit, DEPTH_ACCURACY).ifPresent(occ::setDepthAccuracy);
    getDoubleValue(hit, ELEVATION).ifPresent(occ::setElevation);
    getDoubleValue(hit, ELEVATION_ACCURACY).ifPresent(occ::setElevationAccuracy);
    getStringValue(hit, WATER_BODY).ifPresent(occ::setWaterBody);
  }

  private static void setTaxonFields(SearchHit hit, Occurrence occ) {
    getIntValue(hit, KINGDOM_KEY).ifPresent(occ::setKingdomKey);
    getStringValue(hit, KINGDOM).ifPresent(occ::setKingdom);
    getIntValue(hit, PHYLUM_KEY).ifPresent(occ::setPhylumKey);
    getStringValue(hit, PHYLUM).ifPresent(occ::setPhylum);
    getIntValue(hit, CLASS_KEY).ifPresent(occ::setClassKey);
    getStringValue(hit, CLASS).ifPresent(occ::setClazz);
    getIntValue(hit, ORDER_KEY).ifPresent(occ::setOrderKey);
    getStringValue(hit, ORDER).ifPresent(occ::setOrder);
    getIntValue(hit, FAMILY_KEY).ifPresent(occ::setFamilyKey);
    getStringValue(hit, FAMILY).ifPresent(occ::setFamily);
    getIntValue(hit, GENUS_KEY).ifPresent(occ::setGenusKey);
    getStringValue(hit, GENUS).ifPresent(occ::setGenus);
    getIntValue(hit, SUBGENUS_KEY).ifPresent(occ::setSubgenusKey);
    getStringValue(hit, SUBGENUS).ifPresent(occ::setSubgenus);
    getIntValue(hit, SPECIES_KEY).ifPresent(occ::setSpeciesKey);
    getStringValue(hit, SPECIES).ifPresent(occ::setSpecies);
    getStringValue(hit, SCIENTIFIC_NAME).ifPresent(occ::setScientificName);
    getStringValue(hit, SPECIFIC_EPITHET).ifPresent(occ::setSpecificEpithet);
    getStringValue(hit, INFRA_SPECIFIC_EPITHET).ifPresent(occ::setInfraspecificEpithet);
    getStringValue(hit, GENERIC_NAME).ifPresent(occ::setGenericName);
    getStringValue(hit, TAXON_RANK).ifPresent(v -> occ.setTaxonRank(Rank.valueOf(v)));
    getIntValue(hit, USAGE_TAXON_KEY).ifPresent(occ::setTaxonKey);
    getIntValue(hit, ACCEPTED_TAXON_KEY).ifPresent(occ::setAcceptedTaxonKey);
    getStringValue(hit, ACCEPTED_SCIENTIFIC_NAME).ifPresent(occ::setAcceptedScientificName);
    getValue(hit, TAXONOMIC_STATUS, TaxonomicStatus::valueOf).ifPresent(occ::setTaxonomicStatus);
  }

  private static void setDatasetFields(SearchHit hit, Occurrence occ) {
    getValue(hit, PUBLISHING_COUNTRY, v -> Country.fromIsoCode(v.toUpperCase()))
        .ifPresent(occ::setPublishingCountry);
    getValue(hit, DATASET_KEY, UUID::fromString).ifPresent(occ::setDatasetKey);
    getValue(hit, INSTALLATION_KEY, UUID::fromString).ifPresent(occ::setInstallationKey);
    getValue(hit, PUBLISHING_ORGANIZATION_KEY, UUID::fromString)
        .ifPresent(occ::setPublishingOrgKey);
    getValue(hit, LICENSE, v -> License.fromString(v).orNull()).ifPresent(occ::setLicense);
    getValue(hit, PROTOCOL, EndpointType::fromString).ifPresent(occ::setProtocol);

    getListValue(hit, NETWORK_KEY)
        .ifPresent(
            v -> occ.setNetworkKeys(v.stream().map(UUID::fromString).collect(Collectors.toList())));
  }

  private static void setCrawlingFields(SearchHit hit, Occurrence occ) {
    getValue(hit, CRAWL_ID, Integer::valueOf).ifPresent(occ::setCrawlId);
    getDateValue(hit, LAST_INTERPRETED).ifPresent(occ::setLastInterpreted);
    getDateValue(hit, LAST_PARSED).ifPresent(occ::setLastParsed);
    getDateValue(hit, LAST_CRAWLED).ifPresent(occ::setLastCrawled);
  }

  private static void parseMultimediaItems(SearchHit hit, Occurrence occ) {
    getObjectsListValue(hit, MEDIA_ITEMS)
        .ifPresent(
            items ->
                occ.setMedia(
                    items.stream()
                        .map(
                            item -> {
                              MediaObject mediaObject = new MediaObject();

                              extractValue(item, "type", MediaType::valueOf)
                                  .ifPresent(mediaObject::setType);
                              extractStringValue(item, "format").ifPresent(mediaObject::setFormat);
                              extractValue(item, "identifier", URI::create)
                                  .ifPresent(mediaObject::setIdentifier);
                              extractStringValue(item, "audience")
                                  .ifPresent(mediaObject::setAudience);
                              extractStringValue(item, "contributor")
                                  .ifPresent(mediaObject::setContributor);
                              extractValue(item, "created", STRING_TO_DATE)
                                  .ifPresent(mediaObject::setCreated);
                              extractStringValue(item, "creator")
                                  .ifPresent(mediaObject::setCreator);
                              extractStringValue(item, "description")
                                  .ifPresent(mediaObject::setDescription);
                              extractStringValue(item, "license")
                                  .map(
                                      license ->
                                          License.fromString(license)
                                              .transform(
                                                  l ->
                                                      Optional.ofNullable(l.getLicenseUrl())
                                                          .orElse(license))
                                              .or(license))
                                  .ifPresent(mediaObject::setLicense);
                              extractStringValue(item, "publisher")
                                  .ifPresent(mediaObject::setPublisher);
                              extractValue(item, "references", URI::create)
                                  .ifPresent(mediaObject::setReferences);
                              extractStringValue(item, "rightsHolder")
                                  .ifPresent(mediaObject::setRightsHolder);
                              extractStringValue(item, "source").ifPresent(mediaObject::setSource);
                              extractStringValue(item, "title").ifPresent(mediaObject::setTitle);

                              return mediaObject;
                            })
                        .collect(Collectors.toList())));
  }

  private static Optional<String> getStringValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Function.identity());
  }

  private static Optional<Integer> getIntValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Integer::valueOf);
  }

  private static Optional<Double> getDoubleValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Double::valueOf);
  }

  private static Optional<Date> getDateValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, STRING_TO_DATE);
  }

  private static Optional<List<String>> getListValue(SearchHit hit, OccurrenceEsField esField) {
    return Optional.ofNullable(hit.getSourceAsMap().get(esField.getFieldName()))
        .map(v -> (List<String>) v)
        .filter(v -> !v.isEmpty());
  }

  private static Optional<List<Map<String, Object>>> getObjectsListValue(SearchHit hit, OccurrenceEsField esField) {
    return Optional.ofNullable(hit.getSourceAsMap().get(esField.getFieldName()))
        .map(v -> (List<Map<String, Object>>) v)
        .filter(v -> !v.isEmpty());
  }

  private static <T> Optional<T> getValue(SearchHit hit, OccurrenceEsField esField, Function<String, T> mapper) {
    String fieldName = esField.getFieldName();
    Map<String, Object> fields = hit.getSourceAsMap();
    if (IS_NESTED.test(esField.getFieldName())) {
      // take all paths till the field name
      String[] paths = esField.getFieldName().split("\\.");
      for (int i = 0; i < paths.length - 1 && fields.containsKey(paths[i]); i++) {
        // update the fields with the current path
        fields = (Map<String, Object>) fields.get(paths[i]);
      }
      // the last path is the field name
      fieldName = paths[paths.length - 1];
    }

    return extractValue(fields, fieldName, mapper);
  }

  private static <T> Optional<T> extractValue(Map<String, Object> fields, String fieldName, Function<String, T> mapper) {
      return Optional.ofNullable(fields.get(fieldName)).map(String::valueOf).filter(v -> !v.isEmpty())
        .map(v -> {
          try {
            return mapper.apply(v);
          } catch (Exception ex) {
            LOG.warn("Error extracting field {} with value {}", fieldName, v);
            return null;
          }
        });
  }

  private static Optional<String> extractStringValue(Map<String, Object> fields, String fieldName) {
    return extractValue(fields, fieldName, Function.identity());
  }

  private static Map<Term, String> extractVerbatimFields(SearchHit hit, boolean excludeInterpreted) {
    Map<String, Object> verbatimFields = (Map<String, Object>) hit.getSourceAsMap().get("verbatim");
    Map<String, String> verbatimCoreFields = (Map<String, String>) verbatimFields.get("core");
    Stream<AbstractMap.SimpleEntry<Term, String>> termMap =
    verbatimCoreFields.entrySet().stream()
      .map(e -> new SimpleEntry<>(mapTerm(e.getKey()), e.getValue()));
    if (excludeInterpreted) {
      termMap = termMap.filter(e -> !TermUtils.isInterpretedSourceTerm(e.getKey()));
    }
    return termMap.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Re-maps terms to handle Unknown terms.
   * This has to be done because Pipelines preserve Unknown terms and do not add the URI for unknown terms.
   */
  private static Term mapTerm(String verbatimTerm) {
    Term term  = TERM_FACTORY.findTerm(verbatimTerm);
    if (term instanceof UnknownTerm) {
      return UnknownTerm.build(term.simpleName(), false);
    }
    return term;
  }

}
