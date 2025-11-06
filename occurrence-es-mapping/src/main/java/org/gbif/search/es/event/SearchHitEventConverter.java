/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.search.es.event;

import static org.gbif.search.es.event.EventEsField.*;
import static org.gbif.search.es.event.EventEsField.CRAWL_ID;
import static org.gbif.search.es.event.EventEsField.HOSTING_ORGANIZATION_KEY;
import static org.gbif.search.es.event.EventEsField.LAST_CRAWLED;
import static org.gbif.search.es.event.EventEsField.LAST_INTERPRETED;
import static org.gbif.search.es.event.EventEsField.LAST_PARSED;
import static org.gbif.search.es.event.EventEsField.MEDIA_ITEMS;
import static org.gbif.search.es.event.EventEsField.NETWORK_KEY;
import static org.gbif.search.es.event.EventEsField.PROTOCOL;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.net.URI;
import java.text.ParseException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.SearchHit;
import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.event.Event;
import org.gbif.api.model.event.Humboldt;
import org.gbif.api.model.occurrence.Gadm;
import org.gbif.api.model.occurrence.GadmFeature;
import org.gbif.api.model.occurrence.OccurrenceRelation;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.v2.RankedName;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.EventIssue;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.predicate.query.EsField;
import org.gbif.search.es.SearchHitConverter;

public class SearchHitEventConverter extends SearchHitConverter<Event> {

  private static final Set<Term> EVENT_INTERPRETED_TERMS =
      ImmutableSet.of(DwcTerm.eventID, DwcTerm.parentEventID);

  private final EventEsFieldMapper eventEsFieldMapper;
  private final boolean excludeInterpretedFromVerbatim;

  public SearchHitEventConverter(
      EventEsFieldMapper eventEsFieldMapper, boolean excludeInterpretedFromVerbatim) {
    this.eventEsFieldMapper = eventEsFieldMapper;
    this.excludeInterpretedFromVerbatim = excludeInterpretedFromVerbatim;
  }

  @Override
  public Event apply(SearchHit hit) {
    // create occurrence
    Event event = new Event();

    // set fields
    setEventFields(hit, event);
    setLocationFields(hit, event);
    setTemporalFields(hit, event);
    setCrawlingFields(hit, event);
    setDatasetFields(hit, event);

    // issues
    getListValue(hit, ISSUE)
        .ifPresent(
            v ->
                event.setIssues(
                    v.stream()
                        .map(issue -> VocabularyUtils.lookup(issue, EventIssue.class))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toSet())));

    // multimedia extension
    parseMultimediaItems(hit, event);

    // humboldt extension
    parseHumboldtItems(hit, event);

    // add verbatim fields
    event.getVerbatimFields().putAll(extractVerbatimFields(hit));

    // add verbatim fields
    getMapValue(hit, VERBATIM)
        .ifPresent(
            verbatimData -> {
              if (verbatimData.containsKey("extensions")) {
                event.setExtensions(
                    parseExtensionsMap((Map<String, Object>) verbatimData.get("extensions")));
              }
            });

    setIdentifier(hit, event);

    return event;
  }

  private Map<Term, String> extractVerbatimFields(SearchHit hit) {
    Map<String, Object> verbatimFields = (Map<String, Object>) hit.getSourceAsMap().get("verbatim");
    if (verbatimFields == null) {
      return Collections.emptyMap();
    }
    Map<String, String> verbatimCoreFields = (Map<String, String>) verbatimFields.get("core");
    Stream<AbstractMap.SimpleEntry<Term, String>> termMap =
        verbatimCoreFields.entrySet().stream()
            .map(e -> new AbstractMap.SimpleEntry<>(mapTerm(e.getKey()), e.getValue()));
    if (excludeInterpretedFromVerbatim) {
      termMap =
          termMap.filter(
              e ->
                  !TermUtils.isInterpretedSourceTerm(e.getKey())
                      && !EVENT_INTERPRETED_TERMS.contains(e.getKey()));
    }
    return termMap.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /** Transforms a SearchHit into a suitable Verbatim map of terms. */
  public VerbatimOccurrence toVerbatim(SearchHit hit) {
    VerbatimOccurrence vOcc = new VerbatimOccurrence();
    getValue(hit, PUBLISHING_COUNTRY, v -> Country.fromIsoCode(v.toUpperCase()))
        .ifPresent(vOcc::setPublishingCountry);
    getValue(hit, DATASET_KEY, UUID::fromString).ifPresent(vOcc::setDatasetKey);
    getValue(hit, INSTALLATION_KEY, UUID::fromString).ifPresent(vOcc::setInstallationKey);
    getValue(hit, PUBLISHING_ORGANIZATION_KEY, UUID::fromString)
        .ifPresent(vOcc::setPublishingOrgKey);
    getValue(hit, HOSTING_ORGANIZATION_KEY, UUID::fromString)
        .ifPresent(vOcc::setHostingOrganizationKey);
    getValue(hit, PROTOCOL, EndpointType::fromString).ifPresent(vOcc::setProtocol);

    getListValue(hit, NETWORK_KEY)
        .ifPresent(
            v ->
                vOcc.setNetworkKeys(v.stream().map(UUID::fromString).collect(Collectors.toList())));
    getValue(hit, CRAWL_ID, Integer::valueOf).ifPresent(vOcc::setCrawlId);
    getDateValue(hit, LAST_PARSED).ifPresent(vOcc::setLastParsed);
    getDateValue(hit, LAST_CRAWLED).ifPresent(vOcc::setLastCrawled);
    getValue(hit, GBIF_ID, Long::valueOf)
        .ifPresent(
            id -> {
              vOcc.setKey(id);
              vOcc.getVerbatimFields().put(GbifTerm.gbifID, String.valueOf(id));
            });

    setIdentifier(hit, vOcc);

    // add verbatim fields
    getMapValue(hit, VERBATIM)
        .ifPresent(
            verbatimData -> {
              vOcc.getVerbatimFields()
                  .putAll(parseVerbatimTermMap((Map<String, Object>) (verbatimData).get("core")));
              if (verbatimData.containsKey("extensions")) {
                vOcc.setExtensions(
                    parseExtensionsMap((Map<String, Object>) verbatimData.get("extensions")));
              }
            });

    return vOcc;
  }

  private Map<String, List<Map<Term, String>>> parseExtensionsMap(Map<String, Object> extensions) {
    // parse extensions
    Map<String, List<Map<Term, String>>> extTerms = Maps.newHashMap();
    for (String rowType : extensions.keySet()) {
      List<Map<Term, String>> records = new ArrayList<>();
      // transform records to term based map
      for (Map<String, Object> rawRecord : (List<Map<String, Object>>) extensions.get(rowType)) {
        records.add(parseVerbatimTermMap(rawRecord));
      }
      extTerms.put(rowType, records);
    }
    return extTerms;
  }

  /**
   * Parses a simple string based map into a Term based map, ignoring any non term entries and not
   * parsing nested e.g. extensions data. This produces a Map of verbatim data.
   */
  private Map<Term, String> parseVerbatimTermMap(Map<String, Object> data) {

    Map<Term, String> terms = Maps.newHashMap();
    data.forEach(
        (simpleTermName, value) -> {
          if (Objects.nonNull(value) && !simpleTermName.equalsIgnoreCase("extensions")) {
            Term term = TERM_FACTORY.findTerm(simpleTermName);
            terms.put(term, value.toString());
          }
        });

    return terms;
  }

  /**
   * The id (the <id> reference in the DWCA meta.xml) is an identifier local to the DWCA, and could
   * only have been used for "un-starring" a DWCA star record. However, we've exposed it as
   * DcTerm.identifier for a long time in our public API v1, so we continue to do this.
   */
  private void setIdentifier(SearchHit hit, VerbatimOccurrence event) {

    String institutionCode = event.getVerbatimField(DwcTerm.institutionCode);
    String collectionCode = event.getVerbatimField(DwcTerm.collectionCode);
    String catalogNumber = event.getVerbatimField(DwcTerm.catalogNumber);

    // id format following the convention of DwC (http://rs.tdwg.org/dwc/terms/#occurrenceID)
    String triplet =
        String.join(":", "urn:catalog", institutionCode, collectionCode, catalogNumber);

    String gbifId = Optional.ofNullable(event.getKey()).map(x -> Long.toString(x)).orElse("");
    String occId = event.getVerbatimField(DwcTerm.occurrenceID);

    getStringValue(hit, ID)
        .filter(k -> !k.equals(gbifId) && (!Strings.isNullOrEmpty(occId) || !k.equals(triplet)))
        .ifPresent(result -> event.getVerbatimFields().put(DcTerm.identifier, result));
  }

  private void setEventFields(SearchHit hit, Event event) {
    getValue(hit, GBIF_ID, Long::valueOf)
        .ifPresent(
            id -> {
              event.setKey(id);
              event.getVerbatimFields().put(GbifTerm.gbifID, String.valueOf(id));
            });

    getDateValue(hit, MODIFIED).ifPresent(event::setModified);
    getValue(hit, REFERENCES, URI::create).ifPresent(event::setReferences);
    getStringValue(hit, IDENTIFIER)
        .ifPresent(
            v -> {
              Identifier identifier = new Identifier();
              identifier.setIdentifier(v);
              event.setIdentifiers(Collections.singletonList(identifier));
            });

    getStringValue(hit, RELATION)
        .ifPresent(
            v -> {
              OccurrenceRelation occRelation = new OccurrenceRelation();
              occRelation.setId(v);
              event.setRelations(Collections.singletonList(occRelation));
            });
    getStringValue(hit, PROJECT_ID).ifPresent(event::setProjectId);
    getStringValue(hit, PROGRAMME).ifPresent(event::setProgrammeAcronym);

    getStringValue(hit, SAMPLE_SIZE_UNIT).ifPresent(event::setSampleSizeUnit);
    getDoubleValue(hit, SAMPLE_SIZE_VALUE).ifPresent(event::setSampleSizeValue);
    getDoubleValue(hit, ORGANISM_QUANTITY).ifPresent(event::setOrganismQuantity);
    getStringValue(hit, ORGANISM_QUANTITY_TYPE).ifPresent(event::setOrganismQuantityType);
    getDoubleValue(hit, RELATIVE_ORGANISM_QUANTITY).ifPresent(event::setRelativeOrganismQuantity);
    getListValueAsString(hit, DATASET_ID).ifPresent(event::setDatasetID);
    getListValueAsString(hit, DATASET_NAME).ifPresent(event::setDatasetName);
    getListValueAsString(hit, PREPARATIONS).ifPresent(event::setPreparations);
    getListValueAsString(hit, SAMPLING_PROTOCOL).ifPresent(event::setSamplingProtocol);
    getListValueAsString(hit, OTHER_CATALOG_NUMBERS).ifPresent(event::setOtherCatalogNumbers);
    setEventLineageData(hit, event);
  }

  private void setTemporalFields(SearchHit hit, Event event) {
    getDateValue(hit, DATE_IDENTIFIED).ifPresent(event::setDateIdentified);
    getValue(hit, DAY, Integer::valueOf).ifPresent(event::setDay);
    getValue(hit, MONTH, Integer::valueOf).ifPresent(event::setMonth);
    getValue(hit, YEAR, Integer::valueOf).ifPresent(event::setYear);
    getStringValue(hit, EVENT_DATE_INTERVAL)
        .ifPresent(
            m -> {
              try {
                event.setEventDate(IsoDateInterval.fromString(m));
              } catch (ParseException e) {
              }
            });
    getValue(hit, START_DAY_OF_YEAR, Integer::valueOf).ifPresent(event::setStartDayOfYear);
    getValue(hit, END_DAY_OF_YEAR, Integer::valueOf).ifPresent(event::setEndDayOfYear);
  }

  private void setLocationFields(SearchHit hit, Event event) {
    getValue(hit, CONTINENT, Continent::valueOf).ifPresent(event::setContinent);
    getStringValue(hit, STATE_PROVINCE).ifPresent(event::setStateProvince);
    getValue(hit, COUNTRY_CODE, Country::fromIsoCode).ifPresent(event::setCountry);
    getDoubleValue(hit, COORDINATE_ACCURACY).ifPresent(event::setCoordinateAccuracy);
    getDoubleValue(hit, COORDINATE_PRECISION).ifPresent(event::setCoordinatePrecision);
    getDoubleValue(hit, COORDINATE_UNCERTAINTY_IN_METERS)
        .ifPresent(event::setCoordinateUncertaintyInMeters);
    getDoubleValue(hit, LATITUDE).ifPresent(event::setDecimalLatitude);
    getDoubleValue(hit, LONGITUDE).ifPresent(event::setDecimalLongitude);
    getDoubleValue(hit, DEPTH).ifPresent(event::setDepth);
    getDoubleValue(hit, DEPTH_ACCURACY).ifPresent(event::setDepthAccuracy);
    getDoubleValue(hit, ELEVATION).ifPresent(event::setElevation);
    getDoubleValue(hit, ELEVATION_ACCURACY).ifPresent(event::setElevationAccuracy);
    getStringValue(hit, WATER_BODY).ifPresent(event::setWaterBody);
    getDoubleValue(hit, DISTANCE_FROM_CENTROID_IN_METERS)
        .ifPresent(event::setDistanceFromCentroidInMeters);

    Gadm g = new Gadm();
    getStringValue(hit, GADM_LEVEL_0_GID)
        .ifPresent(
            gid -> {
              g.setLevel0(new GadmFeature());
              g.getLevel0().setGid(gid);
            });
    getStringValue(hit, GADM_LEVEL_1_GID)
        .ifPresent(
            gid -> {
              g.setLevel1(new GadmFeature());
              g.getLevel1().setGid(gid);
            });
    getStringValue(hit, GADM_LEVEL_2_GID)
        .ifPresent(
            gid -> {
              g.setLevel2(new GadmFeature());
              g.getLevel2().setGid(gid);
            });
    getStringValue(hit, GADM_LEVEL_3_GID)
        .ifPresent(
            gid -> {
              g.setLevel3(new GadmFeature());
              g.getLevel3().setGid(gid);
            });
    getStringValue(hit, GADM_LEVEL_0_NAME).ifPresent(name -> g.getLevel0().setName(name));
    getStringValue(hit, GADM_LEVEL_1_NAME).ifPresent(name -> g.getLevel1().setName(name));
    getStringValue(hit, GADM_LEVEL_2_NAME).ifPresent(name -> g.getLevel2().setName(name));
    getStringValue(hit, GADM_LEVEL_3_NAME).ifPresent(name -> g.getLevel3().setName(name));

    event.setGadm(g);
  }

  private void setDatasetFields(SearchHit hit, Event event) {
    getValue(hit, PUBLISHING_COUNTRY, v -> Country.fromIsoCode(v.toUpperCase()))
        .ifPresent(event::setPublishingCountry);
    getValue(hit, DATASET_KEY, UUID::fromString).ifPresent(event::setDatasetKey);
    getValue(hit, INSTALLATION_KEY, UUID::fromString).ifPresent(event::setInstallationKey);
    getValue(hit, PUBLISHING_ORGANIZATION_KEY, UUID::fromString)
        .ifPresent(event::setPublishingOrgKey);
    getValue(hit, LICENSE, v -> License.fromString(v).orElse(null)).ifPresent(event::setLicense);
    getValue(hit, PROTOCOL, EndpointType::fromString).ifPresent(event::setProtocol);
    getValue(hit, HOSTING_ORGANIZATION_KEY, UUID::fromString)
        .ifPresent(event::setHostingOrganizationKey);

    getListValue(hit, NETWORK_KEY)
        .ifPresent(
            v ->
                event.setNetworkKeys(
                    v.stream().map(UUID::fromString).collect(Collectors.toList())));
  }

  private void setCrawlingFields(SearchHit hit, Event event) {
    getValue(hit, CRAWL_ID, Integer::valueOf).ifPresent(event::setCrawlId);
    getDateValue(hit, LAST_INTERPRETED).ifPresent(event::setLastInterpreted);
    getDateValue(hit, LAST_PARSED).ifPresent(event::setLastParsed);
    getDateValue(hit, LAST_CRAWLED).ifPresent(event::setLastCrawled);
  }

  private void parseMultimediaItems(SearchHit hit, Event event) {

    Function<Map<String, Object>, MediaObject> mapFn =
        m -> {
          MediaObject mediaObject = new MediaObject();

          extractStringValue(m, "type", MediaType::valueOf).ifPresent(mediaObject::setType);
          extractStringValue(m, "identifier", URI::create).ifPresent(mediaObject::setIdentifier);
          extractStringValue(m, "references", URI::create).ifPresent(mediaObject::setReferences);
          extractStringValue(m, "created", STRING_TO_DATE).ifPresent(mediaObject::setCreated);
          extractStringValue(m, "format").ifPresent(mediaObject::setFormat);
          extractStringValue(m, "audience").ifPresent(mediaObject::setAudience);
          extractStringValue(m, "contributor").ifPresent(mediaObject::setContributor);
          extractStringValue(m, "creator").ifPresent(mediaObject::setCreator);
          extractStringValue(m, "description").ifPresent(mediaObject::setDescription);
          extractStringValue(m, "publisher").ifPresent(mediaObject::setPublisher);
          extractStringValue(m, "rightsHolder").ifPresent(mediaObject::setRightsHolder);
          extractStringValue(m, "source").ifPresent(mediaObject::setSource);
          extractStringValue(m, "title").ifPresent(mediaObject::setTitle);
          extractStringValue(m, "license")
              .map(
                  license ->
                      License.fromString(license)
                          .map(l -> Optional.ofNullable(l.getLicenseUrl()).orElse(license))
                          .orElse(license))
              .ifPresent(mediaObject::setLicense);

          return mediaObject;
        };

    getObjectsListValue(hit, MEDIA_ITEMS)
        .map(i -> i.stream().map(mapFn).collect(Collectors.toList()))
        .ifPresent(event::setMedia);
  }

  private void parseHumboldtItems(SearchHit hit, Event event) {
    // get the humboldt field name
    Function<EsField, String> fn = e -> e.getValueFieldName().replace("event.humboldt.", "");

    Function<Map<String, Object>, Humboldt> mapFn =
        h -> {
          Humboldt humboldt = new Humboldt();
          getValue(h, fn.apply(HUMBOLDT_SITE_COUNT), Integer::parseInt)
              .ifPresent(humboldt::setSiteCount);
          getListValue(h, fn.apply(HUMBOLDT_VERBATIM_SITE_DESCRIPTIONS))
              .ifPresent(humboldt::setVerbatimSiteDescriptions);
          getListValue(h, fn.apply(HUMBOLDT_VERBATIM_SITE_NAMES))
              .ifPresent(humboldt::setVerbatimSiteNames);
          getDoubleValue(h, fn.apply(HUMBOLDT_GEOSPATIAL_SCOPE_AREA_VALUE))
              .ifPresent(humboldt::setGeospatialScopeAreaValue);
          getStringValue(h, fn.apply(HUMBOLDT_GEOSPATIAL_SCOPE_AREA_UNIT))
              .ifPresent(humboldt::setGeospatialScopeAreaUnit);
          getDoubleValue(h, fn.apply(HUMBOLDT_TOTAL_AREA_SAMPLED_VALUE))
              .ifPresent(humboldt::setTotalAreaSampledValue);
          getStringValue(h, fn.apply(HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT))
              .ifPresent(humboldt::setTotalAreaSampledUnit);
          getListValue(h, fn.apply(HUMBOLDT_TARGET_HABITAT_SCOPE))
              .ifPresent(humboldt::setTargetHabitatScope);
          getListValue(h, fn.apply(HUMBOLDT_EXCLUDED_HABITAT_SCOPE))
              .ifPresent(humboldt::setExcludedHabitatScope);
          getDoubleValue(h, fn.apply(HUMBOLDT_EVENT_DURATION_VALUE))
              .ifPresent(humboldt::setEventDurationValue);
          getStringValue(h, fn.apply(HUMBOLDT_EVENT_DURATION_UNIT))
              .ifPresent(humboldt::setEventDurationUnit);
          createHumboldtTaxonClassification(h, fn.apply(HUMBOLDT_TARGET_TAXONOMIC_SCOPE))
              .ifPresent(humboldt::setTargetTaxonomicScope);
          createHumboldtTaxonClassification(h, fn.apply(HUMBOLDT_EXCLUDED_TAXONOMIC_SCOPE))
              .ifPresent(humboldt::setExcludedTaxonomicScope);
          getListValue(h, fn.apply(HUMBOLDT_TAXON_COMPLETENESS_PROTOCOLS))
              .ifPresent(humboldt::setTaxonCompletenessProtocols);
          getBooleanValue(h, fn.apply(HUMBOLDT_IS_TAXONOMIC_SCOPE_FULLY_REPORTED))
              .ifPresent(humboldt::setIsTaxonomicScopeFullyReported);
          getBooleanValue(h, fn.apply(HUMBOLDT_IS_ABSENCE_REPORTED))
              .ifPresent(humboldt::setIsAbsenceReported);
          createHumboldtTaxonClassification(h, fn.apply(HUMBOLDT_ABSENT_TAXA))
              .ifPresent(humboldt::setAbsentTaxa);
          getBooleanValue(h, fn.apply(HUMBOLDT_HAS_NON_TARGET_TAXA))
              .ifPresent(humboldt::setHasNonTargetTaxa);
          createHumboldtTaxonClassification(h, fn.apply(HUMBOLDT_NON_TARGET_TAXA))
              .ifPresent(humboldt::setNonTargetTaxa);
          getBooleanValue(h, fn.apply(HUMBOLDT_ARE_NON_TARGET_TAXA_FULLY_REPORTED))
              .ifPresent(humboldt::setAreNonTargetTaxaFullyReported);
          getListValue(h, fn.apply(HUMBOLDT_TARGET_LIFE_STAGE_SCOPE))
              .ifPresent(humboldt::setTargetLifeStageScope);
          getListValue(h, fn.apply(HUMBOLDT_EXCLUDED_LIFE_STAGE_SCOPE))
              .ifPresent(humboldt::setExcludedLifeStageScope);
          getBooleanValue(h, fn.apply(HUMBOLDT_IS_LIFE_STAGE_SCOPE_FULLY_REPORTED))
              .ifPresent(humboldt::setIsLifeStageScopeFullyReported);
          getListValue(h, fn.apply(HUMBOLDT_TARGET_DEGREE_OF_ESTABLISHMENT_SCOPE))
              .ifPresent(humboldt::setTargetDegreeOfEstablishmentScope);
          getListValue(h, fn.apply(HUMBOLDT_EXCLUDED_DEGREE_OF_ESTABLISHMENT_SCOPE))
              .ifPresent(humboldt::setExcludedDegreeOfEstablishmentScope);
          getBooleanValue(h, fn.apply(HUMBOLDT_IS_DEGREE_OF_ESTABLISHMENT_SCOPE_FULLY_REPORTED))
              .ifPresent(humboldt::setIsDegreeOfEstablishmentScopeFullyReported);
          getListValue(h, fn.apply(HUMBOLDT_TARGET_GROWTH_FORM_SCOPE))
              .ifPresent(humboldt::setTargetGrowthFormScope);
          getListValue(h, fn.apply(HUMBOLDT_EXCLUDED_GROWTH_FORM_SCOPE))
              .ifPresent(humboldt::setExcludedGrowthFormScope);
          getBooleanValue(h, fn.apply(HUMBOLDT_IS_GROWTH_FORM_SCOPE_FULLY_REPORTED))
              .ifPresent(humboldt::setIsGrowthFormScopeFullyReported);
          getBooleanValue(h, fn.apply(HUMBOLDT_HAS_NON_TARGET_ORGANISMS))
              .ifPresent(humboldt::setHasNonTargetOrganisms);
          getListValue(h, fn.apply(HUMBOLDT_COMPILATION_TYPES))
              .ifPresent(humboldt::setCompilationTypes);
          getListValue(h, fn.apply(HUMBOLDT_COMPILATION_SOURCE_TYPES))
              .ifPresent(humboldt::setCompilationSourceTypes);
          getListValue(h, fn.apply(HUMBOLDT_INVENTORY_TYPES))
              .ifPresent(humboldt::setInventoryTypes);
          getListValue(h, fn.apply(HUMBOLDT_PROTOCOL_NAMES)).ifPresent(humboldt::setProtocolNames);
          getListValue(h, fn.apply(HUMBOLDT_PROTOCOL_DESCRIPTIONS))
              .ifPresent(humboldt::setProtocolDescriptions);
          getListValue(h, fn.apply(HUMBOLDT_PROTOCOL_REFERENCES))
              .ifPresent(humboldt::setProtocolReferences);
          getBooleanValue(h, fn.apply(HUMBOLDT_IS_ABUNDANCE_REPORTED))
              .ifPresent(humboldt::setIsAbundanceReported);
          getBooleanValue(h, fn.apply(HUMBOLDT_IS_ABUNDANCE_CAP_REPORTED))
              .ifPresent(humboldt::setIsAbundanceCapReported);
          getValue(h, fn.apply(HUMBOLDT_ABUNDANCE_CAP), Integer::parseInt)
              .ifPresent(humboldt::setAbundanceCap);
          getBooleanValue(h, fn.apply(HUMBOLDT_IS_VEGETATION_COVER_REPORTED))
              .ifPresent(humboldt::setIsVegetationCoverReported);
          getBooleanValue(
                  h, fn.apply(HUMBOLDT_IS_LEAST_SPECIFIC_TARGET_CATEGORY_QUANTITY_INCLUSIVE))
              .ifPresent(humboldt::setIsLeastSpecificTargetCategoryQuantityInclusive);
          getBooleanValue(h, fn.apply(HUMBOLDT_HAS_VOUCHERS)).ifPresent(humboldt::setHasVouchers);
          getListValue(h, fn.apply(HUMBOLDT_VOUCHER_INSTITUTIONS))
              .ifPresent(humboldt::setVoucherInstitutions);
          getBooleanValue(h, fn.apply(HUMBOLDT_HAS_MATERIAL_SAMPLES))
              .ifPresent(humboldt::setHasMaterialSamples);
          getListValue(h, fn.apply(HUMBOLDT_MATERIAL_SAMPLE_TYPES))
              .ifPresent(humboldt::setMaterialSampleTypes);
          getListValue(h, fn.apply(HUMBOLDT_SAMPLING_PERFORMED_BY))
              .ifPresent(humboldt::setSamplingPerformedBy);
          getBooleanValue(h, fn.apply(HUMBOLDT_IS_SAMPLING_EFFORT_REPORTED))
              .ifPresent(humboldt::setIsSamplingEffortReported);
          getDoubleValue(h, fn.apply(HUMBOLDT_SAMPLING_EFFORT_VALUE))
              .ifPresent(humboldt::setSamplingEffortValue);
          getStringValue(h, fn.apply(HUMBOLDT_SAMPLING_EFFORT_UNIT))
              .ifPresent(humboldt::setSamplingEffortUnit);

          return humboldt;
        };

    getObjectsListValue(hit, HUMBOLDT_ITEMS)
        .map(i -> i.stream().map(mapFn).collect(Collectors.toList()))
        .ifPresent(event::setHumboldt);
  }

  private Optional<Map<String, List<Humboldt.TaxonClassification>>>
      createHumboldtTaxonClassification(Map<String, Object> fields, String fieldName) {
    return getMapValue(fields, fieldName)
        .map(
            c ->
                c.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey,
                            classification -> {
                              List<Map<String, Object>> values =
                                  (List<Map<String, Object>>) classification.getValue();
                              List<Humboldt.TaxonClassification> taxonClassifications =
                                  new ArrayList<>();
                              if (values != null && !values.isEmpty()) {
                                values.forEach(
                                    value -> {
                                      Humboldt.TaxonClassification taxonClassification =
                                          new Humboldt.TaxonClassification();
                                      taxonClassification.setUsageKey(
                                          (String) value.get("usageKey"));
                                      taxonClassification.setUsageName(
                                          (String) value.get("usageName"));
                                      taxonClassification.setUsageRank(
                                          (String) value.get("usageRank"));
                                      taxonClassification.setIssues(
                                          (List<String>) value.get("issues"));

                                      Map<String, String> classificationKeys =
                                          (Map<String, String>) value.get("classificationKeys");
                                      taxonClassification.setClassification(
                                          ((Map<String, String>) value.get("classification"))
                                              .entrySet().stream()
                                                  .map(
                                                      e ->
                                                          new RankedName(
                                                              classificationKeys.get(e.getKey()),
                                                              e.getValue(),
                                                              e.getKey(),
                                                              null))
                                                  .collect(Collectors.toList()));

                                      taxonClassifications.add(taxonClassification);
                                    });
                              }
                              return taxonClassifications;
                            })));
  }

  private void setEventLineageData(SearchHit hit, Event event) {
    event.setId(hit.getId());
    getStringValue(hit, EventEsField.EVENT_ID).ifPresent(event::setEventID);
    getStringValue(hit, EventEsField.PARENT_EVENT_ID).ifPresent(event::setParentEventID);
    getStringValue(hit, EVENT_TYPE).ifPresent(event::setEventType);
    getObjectsListValue(hit, EventEsField.PARENTS_LINEAGE)
        .map(
            v ->
                v.stream()
                    .map(
                        l ->
                            new Event.ParentLineage(
                                (String) l.get("id"), (String) l.get("eventType")))
                    .collect(Collectors.toList()))
        .ifPresent(event::setParentsLineage);
  }
}
