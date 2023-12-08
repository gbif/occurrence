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
package org.gbif.event.search.es;

import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.event.Event;
import org.gbif.api.model.occurrence.AgentIdentifier;
import org.gbif.api.model.occurrence.Gadm;
import org.gbif.api.model.occurrence.GadmFeature;
import org.gbif.api.model.occurrence.OccurrenceRelation;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.AgentIdentifierType;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.api.vocabulary.Sex;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.search.es.EsField;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.occurrence.search.es.SearchHitConverter;

import java.net.URI;
import java.text.ParseException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.common.Strings;
import org.elasticsearch.search.SearchHit;

import com.google.common.collect.Maps;

import static org.gbif.event.search.es.EventEsField.*;
import static org.gbif.event.search.es.EventEsField.CRAWL_ID;
import static org.gbif.event.search.es.EventEsField.HOSTING_ORGANIZATION_KEY;
import static org.gbif.event.search.es.EventEsField.LAST_CRAWLED;
import static org.gbif.event.search.es.EventEsField.LAST_INTERPRETED;
import static org.gbif.event.search.es.EventEsField.LAST_PARSED;
import static org.gbif.event.search.es.EventEsField.MEDIA_ITEMS;
import static org.gbif.event.search.es.EventEsField.NETWORK_KEY;
import static org.gbif.event.search.es.EventEsField.PROTOCOL;
import static org.gbif.occurrence.search.es.OccurrenceEsField.IDENTIFIED_BY_ID;
import static org.gbif.occurrence.search.es.OccurrenceEsField.RECORDED_BY_ID;

public class SearchHitEventConverter extends SearchHitConverter<Event> {


  private final boolean excludeInterpretedFromVerbatim;

  public SearchHitEventConverter(OccurrenceBaseEsFieldMapper occurrenceBaseEsFieldMapper, boolean excludeInterpretedFromVerbatim) {
    super(occurrenceBaseEsFieldMapper);
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
    setGrscicollFields(hit, event);

    // issues
    getListValue(hit, ISSUE)
      .ifPresent(
        v ->
          event.setIssues(
            v.stream().map(issue -> VocabularyUtils.lookup(issue, OccurrenceIssue.class))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toSet())));

    // multimedia extension
    parseMultimediaItems(hit, event);

    parseAgentIds(hit, event);

    // add verbatim fields
    event.getVerbatimFields().putAll(extractVerbatimFields(hit));

    // add verbatim fields
    getMapValue(hit, VERBATIM).ifPresent(verbatimData -> {
      if (verbatimData.containsKey("extensions" )) {
        event.setExtensions(parseExtensionsMap((Map<String, Object>)verbatimData.get("extensions")));
      }
    });

    setIdentifier(hit, event);

    return event;
  }

  private Map<Term, String> extractVerbatimFields(SearchHit hit) {
    Map<String, Object> verbatimFields = (Map<String, Object>) hit.getSourceAsMap().get("verbatim");
    if(verbatimFields == null) {
      return Collections.emptyMap();
    }
    Map<String, String> verbatimCoreFields = (Map<String, String>) verbatimFields.get("core");
    Stream<AbstractMap.SimpleEntry<Term, String>> termMap =
      verbatimCoreFields.entrySet().stream()
        .map(e -> new AbstractMap.SimpleEntry<>(mapTerm(e.getKey()), e.getValue()));
    if (excludeInterpretedFromVerbatim) {
      termMap = termMap.filter(e -> !TermUtils.isInterpretedSourceTerm(e.getKey()));
    }
    return termMap.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Transforms a SearchHit into a suitable Verbatim map of terms.
   */
  public VerbatimOccurrence toVerbatim(SearchHit hit) {
    VerbatimOccurrence vOcc = new VerbatimOccurrence();
    getValue(hit, PUBLISHING_COUNTRY, v -> Country.fromIsoCode(v.toUpperCase()))
      .ifPresent(vOcc::setPublishingCountry);
    getValue(hit, DATASET_KEY, UUID::fromString).ifPresent(vOcc::setDatasetKey);
    getValue(hit, INSTALLATION_KEY, UUID::fromString).ifPresent(vOcc::setInstallationKey);
    getValue(hit, PUBLISHING_ORGANIZATION_KEY, UUID::fromString).ifPresent(vOcc::setPublishingOrgKey);
    getValue(hit, HOSTING_ORGANIZATION_KEY, UUID::fromString).ifPresent(vOcc::setHostingOrganizationKey);
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

    setIdentifier(hit, vOcc);

    // add verbatim fields
    getMapValue(hit, VERBATIM).ifPresent(verbatimData -> {
      vOcc.getVerbatimFields().putAll(parseVerbatimTermMap((Map<String, Object>)(verbatimData).get("core")));
      if (verbatimData.containsKey("extensions" )) {
        vOcc.setExtensions(parseExtensionsMap((Map<String, Object>)verbatimData.get("extensions")));
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
   * Parses a simple string based map into a Term based map, ignoring any non term entries and not parsing nested
   * e.g. extensions data.
   * This produces a Map of verbatim data.
   */
  private Map<Term, String> parseVerbatimTermMap(Map<String, Object> data) {

    Map<Term, String> terms = Maps.newHashMap();
    data.forEach( (simpleTermName,value) -> {
      if (Objects.nonNull(value) && !simpleTermName.equalsIgnoreCase("extensions")) {
        Term term = TERM_FACTORY.findTerm(simpleTermName);
        terms.put(term, value.toString());
      }
    });

    return terms;
  }

  /**
   * The id (the <id> reference in the DWCA meta.xml) is an identifier local to the DWCA, and could only have been
   * used for "un-starring" a DWCA star record. However, we've exposed it as DcTerm.identifier for a long time in
   * our public API v1, so we continue to do this.
   */
  private void setIdentifier(SearchHit hit, VerbatimOccurrence event) {

    String institutionCode = event.getVerbatimField(DwcTerm.institutionCode);
    String collectionCode = event.getVerbatimField(DwcTerm.collectionCode);
    String catalogNumber = event.getVerbatimField(DwcTerm.catalogNumber);

    // id format following the convention of DwC (http://rs.tdwg.org/dwc/terms/#occurrenceID)
    String triplet = String.join(":", "urn:catalog", institutionCode, collectionCode, catalogNumber);

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
    getValue(hit, BASIS_OF_RECORD, BasisOfRecord::valueOf).ifPresent(event::setBasisOfRecord);
    getStringValue(hit, ESTABLISHMENT_MEANS).ifPresent(event::setEstablishmentMeans);
    getStringValue(hit, LIFE_STAGE).ifPresent(event::setLifeStage);
    getStringValue(hit, DEGREE_OF_ESTABLISHMENT_MEANS).ifPresent(event::setDegreeOfEstablishment);
    getStringValue(hit, PATHWAY).ifPresent(event::setPathway);
    getDateValue(hit, MODIFIED).ifPresent(event::setModified);
    getValue(hit, REFERENCES, URI::create).ifPresent(event::setReferences);
    getValue(hit, SEX, Sex::valueOf).ifPresent(event::setSex);
    getValue(hit, INDIVIDUAL_COUNT, Integer::valueOf).ifPresent(event::setIndividualCount);
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

    getValue(hit, OCCURRENCE_STATUS, OccurrenceStatus::valueOf).ifPresent(event::setOccurrenceStatus);
    getBooleanValue(hit, IS_IN_CLUSTER).ifPresent(event::setInCluster);
    getListValueAsString(hit, DATASET_ID).ifPresent(event::setDatasetID);
    getListValueAsString(hit, DATASET_NAME).ifPresent(event::setDatasetName);
    getListValueAsString(hit, RECORDED_BY).ifPresent(event::setRecordedBy);
    getListValueAsString(hit, IDENTIFIED_BY).ifPresent(event::setIdentifiedBy);
    getListValueAsString(hit, PREPARATIONS).ifPresent(event::setPreparations);
    getListValueAsString(hit, SAMPLING_PROTOCOL).ifPresent(event::setSamplingProtocol);
    getListValueAsString(hit, OTHER_CATALOG_NUMBERS).ifPresent(event::setOtherCatalogNumbers);
  }

  private void parseAgentIds(SearchHit hit, Event event) {
    Function<Map<String, Object>, AgentIdentifier> mapFn = m -> {
      AgentIdentifier ai = new AgentIdentifier();
      extractStringValue(m, "type", AgentIdentifierType::valueOf).ifPresent(ai::setType);
      extractStringValue(m, "value").ifPresent(ai::setValue);
      return ai;
    };

    getObjectsListValue(hit, RECORDED_BY_ID.getSearchFieldName().replace(".value", ""))
      .map(i -> i.stream().map(mapFn).collect(Collectors.toList()))
      .ifPresent(event::setRecordedByIds);

    getObjectsListValue(hit, IDENTIFIED_BY_ID.getSearchFieldName().replace(".value", ""))
      .map(i -> i.stream().map(mapFn).collect(Collectors.toList()))
      .ifPresent(event::setIdentifiedByIds);
  }

  private void setTemporalFields(SearchHit hit, Event event) {
    getDateValue(hit, DATE_IDENTIFIED).ifPresent(event::setDateIdentified);
    getValue(hit, DAY, Integer::valueOf).ifPresent(event::setDay);
    getValue(hit, MONTH, Integer::valueOf).ifPresent(event::setMonth);
    getValue(hit, YEAR, Integer::valueOf).ifPresent(event::setYear);
    getStringValue(hit, EVENT_DATE_INTERVAL).ifPresent(m -> {
      try {
        event.setEventDate(IsoDateInterval.fromString(m));
      } catch (ParseException e) {}
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
    getDoubleValue(hit, COORDINATE_UNCERTAINTY_IN_METERS).ifPresent(event::setCoordinateUncertaintyInMeters);
    getDoubleValue(hit, LATITUDE).ifPresent(event::setDecimalLatitude);
    getDoubleValue(hit, LONGITUDE).ifPresent(event::setDecimalLongitude);
    getDoubleValue(hit, DEPTH).ifPresent(event::setDepth);
    getDoubleValue(hit, DEPTH_ACCURACY).ifPresent(event::setDepthAccuracy);
    getDoubleValue(hit, ELEVATION).ifPresent(event::setElevation);
    getDoubleValue(hit, ELEVATION_ACCURACY).ifPresent(event::setElevationAccuracy);
    getStringValue(hit, WATER_BODY).ifPresent(event::setWaterBody);
    getDoubleValue(hit, DISTANCE_FROM_CENTROID_IN_METERS).ifPresent(event::setDistanceFromCentroidInMeters);

    Gadm g = new Gadm();
    getStringValue(hit, GADM_LEVEL_0_GID).ifPresent(gid -> {
      g.setLevel0(new GadmFeature());
      g.getLevel0().setGid(gid);
    });
    getStringValue(hit, GADM_LEVEL_1_GID).ifPresent(gid -> {
      g.setLevel1(new GadmFeature());
      g.getLevel1().setGid(gid);
    });
    getStringValue(hit, GADM_LEVEL_2_GID).ifPresent(gid -> {
      g.setLevel2(new GadmFeature());
      g.getLevel2().setGid(gid);
    });
    getStringValue(hit, GADM_LEVEL_3_GID).ifPresent(gid -> {
      g.setLevel3(new GadmFeature());
      g.getLevel3().setGid(gid);
    });
    getStringValue(hit, GADM_LEVEL_0_NAME).ifPresent(name -> g.getLevel0().setName(name));
    getStringValue(hit, GADM_LEVEL_1_NAME).ifPresent(name -> g.getLevel1().setName(name));
    getStringValue(hit, GADM_LEVEL_2_NAME).ifPresent(name -> g.getLevel2().setName(name));
    getStringValue(hit, GADM_LEVEL_3_NAME).ifPresent(name -> g.getLevel3().setName(name));

    event.setGadm(g);
    setEventLineageData(hit, event);
  }


  private void setGrscicollFields(SearchHit hit, Event event) {
    getStringValue(hit, INSTITUTION_KEY).ifPresent(event::setInstitutionKey);
    getStringValue(hit, COLLECTION_KEY).ifPresent(event::setCollectionKey);
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
    getValue(hit, HOSTING_ORGANIZATION_KEY, UUID::fromString).ifPresent(event::setHostingOrganizationKey);

    getListValue(hit, NETWORK_KEY)
      .ifPresent(
        v -> event.setNetworkKeys(v.stream().map(UUID::fromString).collect(Collectors.toList())));
  }

  private void setCrawlingFields(SearchHit hit, Event event) {
    getValue(hit, CRAWL_ID, Integer::valueOf).ifPresent(event::setCrawlId);
    getDateValue(hit, LAST_INTERPRETED).ifPresent(event::setLastInterpreted);
    getDateValue(hit, LAST_PARSED).ifPresent(event::setLastParsed);
    getDateValue(hit, LAST_CRAWLED).ifPresent(event::setLastCrawled);
  }

  private void parseMultimediaItems(SearchHit hit, Event event) {

    Function<Map<String, Object>, MediaObject> mapFn = m -> {
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
        .map(license ->
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

  private void setEventLineageData(SearchHit hit, Event event) {
    event.setId(hit.getId());
    getStringValue(hit, EventEsField.EVENT_ID).ifPresent(event::setEventID);
    getStringValue(hit, EventEsField.PARENT_EVENT_ID).ifPresent(event::setParentEventID);
    getEventType(hit).ifPresent(event::setEventType);
    getObjectsListValue(hit, EventEsField.PARENTS_LINEAGE)
      .map(v -> v.stream().map(l -> new Event.ParentLineage((String)l.get("id"), (String)l.get("eventType")))
        .collect(Collectors.toList()))
      .ifPresent(event::setParentsLineage);
  }

  private Optional<Event.VocabularyConcept> getEventType(SearchHit hit) {
    return getMapValue(hit, EventEsField.EVENT_TYPE)
      .map(cm -> new Event.VocabularyConcept((String)cm.get("concept"), new HashSet<>((List<String>)cm.get("lineage"))));
  }
}
