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
package org.gbif.occurrence.search.es;

import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.occurrence.AgentIdentifier;
import org.gbif.api.model.occurrence.Gadm;
import org.gbif.api.model.occurrence.GadmFeature;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.OccurrenceRelation;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.AgentIdentifierType;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.GbifRegion;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.api.vocabulary.Rank;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TaxonomicStatus;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GadmTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.occurrence.common.TermUtils;

import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
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


public class SearchHitOccurrenceConverter extends SearchHitConverter<Occurrence> {

  public SearchHitOccurrenceConverter(OccurrenceBaseEsFieldMapper occurrenceBaseEsFieldMapper, boolean excludeInterpretedFromVerbatim) {
    super(occurrenceBaseEsFieldMapper);
    this.excludeInterpretedFromVerbatim = excludeInterpretedFromVerbatim;
  }

  private final boolean excludeInterpretedFromVerbatim;

  @Override
  public Occurrence apply(SearchHit hit) {
    // create occurrence
    Occurrence occ = new Occurrence();

    // set fields
    setOccurrenceFields(hit, occ);
    setLocationFields(hit, occ);
    setTemporalFields(hit, occ);
    setCrawlingFields(hit, occ);
    setDatasetFields(hit, occ);
    setTaxonFields(hit, occ);
    setGrscicollFields(hit, occ);

    // issues
    getListValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.issue).getValueFieldName())
      .ifPresent(
        v ->
          occ.setIssues(
            v.stream().map(issue -> VocabularyUtils.lookup(issue, OccurrenceIssue.class))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toSet())));

    // multimedia extension
    parseMultimediaItems(hit, occ);

    parseAgentIds(hit, occ);

    // add verbatim fields
    occ.getVerbatimFields().putAll(extractVerbatimFields(hit));

    // add verbatim fields
    getMapValue(hit, occurrenceBaseEsFieldMapper.getEsField(UnknownTerm.build("verbatim")).getValueFieldName()).ifPresent(verbatimData -> {
      if (verbatimData.containsKey("extensions" )) {
        occ.setExtensions(parseExtensionsMap((Map<String, Object>)verbatimData.get("extensions")));
      }
    });

    setIdentifier(hit, occ);

    return occ;
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
  public VerbatimOccurrence toVerbatimOccurrence(SearchHit hit) {
    VerbatimOccurrence vOcc = new VerbatimOccurrence();
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.publishingCountry), v -> Country.fromIsoCode(v.toUpperCase()))
      .ifPresent(vOcc::setPublishingCountry);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.datasetKey), UUID::fromString).ifPresent(vOcc::setDatasetKey);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.installationKey), UUID::fromString).ifPresent(vOcc::setInstallationKey);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.publishingOrgKey), UUID::fromString)
      .ifPresent(vOcc::setPublishingOrgKey);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.protocol), EndpointType::fromString).ifPresent(vOcc::setProtocol);

    getListValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.networkKey).getValueFieldName())
      .ifPresent(
        v -> vOcc.setNetworkKeys(v.stream().map(UUID::fromString).collect(Collectors.toList())));
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.crawlId), Integer::valueOf).ifPresent(vOcc::setCrawlId);
    getDateValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.lastParsed)).ifPresent(vOcc::setLastParsed);
    getDateValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.lastCrawled)).ifPresent(vOcc::setLastCrawled);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.gbifID), Long::valueOf)
      .ifPresent(
        id -> {
          vOcc.setKey(id);
          vOcc.getVerbatimFields().put(GbifTerm.gbifID, String.valueOf(id));
        });

    setIdentifier(hit, vOcc);

    // add verbatim fields
    getMapValue(hit, occurrenceBaseEsFieldMapper.getEsField(EsField.VERBATIM).getValueFieldName()).ifPresent(verbatimData -> {
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
  private void setIdentifier(SearchHit hit, VerbatimOccurrence occ) {

    String institutionCode = occ.getVerbatimField(DwcTerm.institutionCode);
    String collectionCode = occ.getVerbatimField(DwcTerm.collectionCode);
    String catalogNumber = occ.getVerbatimField(DwcTerm.catalogNumber);

    // id format following the convention of DwC (http://rs.tdwg.org/dwc/terms/#occurrenceID)
    String triplet = String.join(":", "urn:catalog", institutionCode, collectionCode, catalogNumber);

    String gbifId = Optional.ofNullable(occ.getKey()).map(x -> Long.toString(x)).orElse("");
    String occId = occ.getVerbatimField(DwcTerm.occurrenceID);

    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DcTerm.identifier))
      .filter(k -> !k.equals(gbifId) && (!Strings.isNullOrEmpty(occId) || !k.equals(triplet)))
      .ifPresent(result -> occ.getVerbatimFields().put(DcTerm.identifier, result));
  }

  private void setOccurrenceFields(SearchHit hit, Occurrence occ) {
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.gbifID), Long::valueOf)
      .ifPresent(
        id -> {
          occ.setKey(id);
          occ.getVerbatimFields().put(GbifTerm.gbifID, String.valueOf(id));
        });
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.basisOfRecord), BasisOfRecord::valueOf).ifPresent(occ::setBasisOfRecord);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.establishmentMeans)).ifPresent(occ::setEstablishmentMeans);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.lifeStage)).ifPresent(occ::setLifeStage);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.degreeOfEstablishment)).ifPresent(occ::setDegreeOfEstablishment);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.pathway)).ifPresent(occ::setPathway);
    getDateValue(hit, occurrenceBaseEsFieldMapper.getEsField(DcTerm.modified)).ifPresent(occ::setModified);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DcTerm.references), URI::create).ifPresent(occ::setReferences);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.sex), Sex::valueOf).ifPresent(occ::setSex);
    getListValueAsString(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.typeStatus).getValueFieldName()).ifPresent(occ::setTypeStatus);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.typifiedName)).ifPresent(occ::setTypifiedName);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.individualCount), Integer::valueOf).ifPresent(occ::setIndividualCount);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DcTerm.identifier))
      .ifPresent(
        v -> {
          Identifier identifier = new Identifier();
          identifier.setIdentifier(v);
          occ.setIdentifiers(Collections.singletonList(identifier));
        });

    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DcTerm.relation))
      .ifPresent(
        v -> {
          OccurrenceRelation occRelation = new OccurrenceRelation();
          occRelation.setId(v);
          occ.setRelations(Collections.singletonList(occRelation));
        });
    getListValueAsString(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.projectId)).ifPresent(occ::setProjectId);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.programmeAcronym)).ifPresent(occ::setProgrammeAcronym);

    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.sampleSizeUnit)).ifPresent(occ::setSampleSizeUnit);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.sampleSizeValue)).ifPresent(occ::setSampleSizeValue);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.organismQuantity)).ifPresent(occ::setOrganismQuantity);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.organismQuantityType)).ifPresent(occ::setOrganismQuantityType);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.relativeOrganismQuantity)).ifPresent(occ::setRelativeOrganismQuantity);
    getBooleanValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.isSequenced)).ifPresent(occ::setIsSequenced);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.fieldNumber)).ifPresent(occ::setFieldNumber);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.occurrenceStatus), OccurrenceStatus::valueOf).ifPresent(occ::setOccurrenceStatus);
    getBooleanValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.isInCluster)).ifPresent(occ::setIsInCluster);
    getListValueAsString(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.datasetID).getValueFieldName()).ifPresent(occ::setDatasetID);
    getListValueAsString(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.datasetName).getValueFieldName()).ifPresent(occ::setDatasetName);
    getListValueAsString(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.recordedBy).getValueFieldName()).ifPresent(occ::setRecordedBy);
    getListValueAsString(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.identifiedBy).getValueFieldName()).ifPresent(occ::setIdentifiedBy);
    getListValueAsString(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.preparations).getValueFieldName()).ifPresent(occ::setPreparations);
    getListValueAsString(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.samplingProtocol).getValueFieldName()).ifPresent(occ::setSamplingProtocol);
    getListValueAsString(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.otherCatalogNumbers).getValueFieldName()).ifPresent(occ::setOtherCatalogNumbers);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.previousIdentifications)).ifPresent(occ::setPreviousIdentifications);

    //Geological context
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.earliestEonOrLowestEonothem)).ifPresent(occ::setEarliestEonOrLowestEonothem);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.latestEonOrHighestEonothem)).ifPresent(occ::setLatestEonOrHighestEonothem);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.earliestEraOrLowestErathem)).ifPresent(occ::setEarliestEraOrLowestErathem);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.latestEraOrHighestErathem)).ifPresent(occ::setLatestEraOrHighestErathem);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.earliestPeriodOrLowestSystem)).ifPresent(occ::setEarliestPeriodOrLowestSystem);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.latestPeriodOrHighestSystem)).ifPresent(occ::setLatestPeriodOrHighestSystem);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.earliestEpochOrLowestSeries)).ifPresent(occ::setEarliestEpochOrLowestSeries);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.latestEpochOrHighestSeries)).ifPresent(occ::setLatestEpochOrHighestSeries);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.earliestAgeOrLowestStage)).ifPresent(occ::setEarliestAgeOrLowestStage);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.latestAgeOrHighestStage)).ifPresent(occ::setLatestAgeOrHighestStage);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.lowestBiostratigraphicZone)).ifPresent(occ::setLowestBiostratigraphicZone);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.highestBiostratigraphicZone)).ifPresent(occ::setHighestBiostratigraphicZone);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.group)).ifPresent(occ::setGroup);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.formation)).ifPresent(occ::setFormation);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.member)).ifPresent(occ::setMember);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.bed)).ifPresent(occ::setBed);
  }

  private void parseAgentIds(SearchHit hit, Occurrence occ) {
    Function<Map<String, Object>, AgentIdentifier> mapFn = m -> {
      AgentIdentifier ai = new AgentIdentifier();
      extractStringValue(m, "type", AgentIdentifierType::valueOf).ifPresent(ai::setType);
      extractStringValue(m, "value").ifPresent(ai::setValue);
      return ai;
    };

    getObjectsListValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.recordedByID).getSearchFieldName().replace(".value", ""))
      .map(i -> i.stream().map(mapFn).collect(Collectors.toList()))
      .ifPresent(occ::setRecordedByIds);

    getObjectsListValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.identifiedByID).getSearchFieldName().replace(".value", ""))
      .map(i -> i.stream().map(mapFn).collect(Collectors.toList()))
      .ifPresent(occ::setIdentifiedByIds);
  }

  private void setTemporalFields(SearchHit hit, Occurrence occ) {
    getDateValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.dateIdentified)).ifPresent(occ::setDateIdentified);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.day), Integer::valueOf).ifPresent(occ::setDay);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.month), Integer::valueOf).ifPresent(occ::setMonth);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.year), Integer::valueOf).ifPresent(occ::setYear);
    getDateValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.eventDate)).ifPresent(occ::setEventDate);
  }

  private void setLocationFields(SearchHit hit, Occurrence occ) {
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.continent), Continent::valueOf).ifPresent(occ::setContinent);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.stateProvince)).ifPresent(occ::setStateProvince);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.countryCode), Country::fromIsoCode).ifPresent(occ::setCountry);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.gbifRegion), GbifRegion::fromString).ifPresent(occ::setGbifRegion);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.coordinateAccuracy)).ifPresent(occ::setCoordinateAccuracy);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.coordinatePrecision)).ifPresent(occ::setCoordinatePrecision);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.coordinateUncertaintyInMeters)).ifPresent(occ::setCoordinateUncertaintyInMeters);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.decimalLatitude)).ifPresent(occ::setDecimalLatitude);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.decimalLongitude)).ifPresent(occ::setDecimalLongitude);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.depth)).ifPresent(occ::setDepth);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.depthAccuracy)).ifPresent(occ::setDepthAccuracy);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.elevation)).ifPresent(occ::setElevation);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.elevationAccuracy)).ifPresent(occ::setElevationAccuracy);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.waterBody)).ifPresent(occ::setWaterBody);
    getDoubleValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.distanceFromCentroidInMeters)).ifPresent(occ::setDistanceFromCentroidInMeters);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.island)).ifPresent(occ::setIsland);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.islandGroup)).ifPresent(occ::setIslandGroup);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.higherGeography)).ifPresent(occ::setHigherGeography);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.georeferencedBy)).ifPresent(occ::setGeoreferencedBy);

    Gadm g = new Gadm();
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GadmTerm.level0Gid)).ifPresent(gid -> {
      g.setLevel0(new GadmFeature());
      g.getLevel0().setGid(gid);
    });
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GadmTerm.level1Gid)).ifPresent(gid -> {
      g.setLevel1(new GadmFeature());
      g.getLevel1().setGid(gid);
    });
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GadmTerm.level2Gid)).ifPresent(gid -> {
      g.setLevel2(new GadmFeature());
      g.getLevel2().setGid(gid);
    });
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GadmTerm.level3Gid)).ifPresent(gid -> {
      g.setLevel3(new GadmFeature());
      g.getLevel3().setGid(gid);
    });
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GadmTerm.level0Name)).ifPresent(name -> g.getLevel0().setName(name));
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GadmTerm.level1Name)).ifPresent(name -> g.getLevel1().setName(name));
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GadmTerm.level2Name)).ifPresent(name -> g.getLevel2().setName(name));
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GadmTerm.level3Name)).ifPresent(name -> g.getLevel3().setName(name));

    occ.setGadm(g);
  }

  private void setTaxonFields(SearchHit hit, Occurrence occ) {
    getIntValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.kingdomKey)).ifPresent(occ::setKingdomKey);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.kingdom)).ifPresent(occ::setKingdom);
    getIntValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.phylumKey)).ifPresent(occ::setPhylumKey);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.phylum)).ifPresent(occ::setPhylum);
    getIntValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.classKey)).ifPresent(occ::setClassKey);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.class_)).ifPresent(occ::setClazz);
    getIntValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.orderKey)).ifPresent(occ::setOrderKey);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.order)).ifPresent(occ::setOrder);
    getIntValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.familyKey)).ifPresent(occ::setFamilyKey);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.family)).ifPresent(occ::setFamily);
    getIntValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.genusKey)).ifPresent(occ::setGenusKey);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.genus)).ifPresent(occ::setGenus);
    getIntValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.subgenusKey)).ifPresent(occ::setSubgenusKey);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.subgenus)).ifPresent(occ::setSubgenus);
    getIntValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.speciesKey)).ifPresent(occ::setSpeciesKey);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.species)).ifPresent(occ::setSpecies);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.scientificName)).ifPresent(occ::setScientificName);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.specificEpithet)).ifPresent(occ::setSpecificEpithet);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.infraspecificEpithet)).ifPresent(occ::setInfraspecificEpithet);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.genericName)).ifPresent(occ::setGenericName);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.taxonRank)).ifPresent(v -> occ.setTaxonRank(Rank.valueOf(v)));
    getIntValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.taxonKey)).ifPresent(occ::setTaxonKey);
    getIntValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.acceptedTaxonKey)).ifPresent(occ::setAcceptedTaxonKey);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.acceptedScientificName)).ifPresent(occ::setAcceptedScientificName);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.taxonomicStatus), TaxonomicStatus::valueOf).ifPresent(occ::setTaxonomicStatus);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(IucnTerm.iucnRedListCategory)).ifPresent(occ::setIucnRedListCategory);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(DwcTerm.taxonConceptID)).ifPresent(occ::setTaxonConceptID);
  }

  private void setGrscicollFields(SearchHit hit, Occurrence occ) {
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.institutionKey)).ifPresent(occ::setInstitutionKey);
    getStringValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.collectionKey)).ifPresent(occ::setCollectionKey);
  }

  private void setDatasetFields(SearchHit hit, Occurrence occ) {
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.publishingCountry), v -> Country.fromIsoCode(v.toUpperCase()))
      .ifPresent(occ::setPublishingCountry);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.publishedByGbifRegion), v -> GbifRegion.fromString(v.toUpperCase()))
      .ifPresent(occ::setPublishedByGbifRegion);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.datasetKey), UUID::fromString).ifPresent(occ::setDatasetKey);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.installationKey), UUID::fromString).ifPresent(occ::setInstallationKey);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.publishingOrgKey), UUID::fromString)
      .ifPresent(occ::setPublishingOrgKey);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(DcTerm.license), v -> License.fromString(v).orElse(null)).ifPresent(occ::setLicense);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.protocol), EndpointType::fromString).ifPresent(occ::setProtocol);
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.hostingOrganizationKey), UUID::fromString).ifPresent(occ::setHostingOrganizationKey);

    getListValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.networkKey).getValueFieldName())
      .ifPresent(
        v -> occ.setNetworkKeys(v.stream().map(UUID::fromString).collect(Collectors.toList())));
  }

  private void setCrawlingFields(SearchHit hit, Occurrence occ) {
    getValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifInternalTerm.crawlId), Integer::valueOf).ifPresent(occ::setCrawlId);
    getDateValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.lastInterpreted)).ifPresent(occ::setLastInterpreted);
    getDateValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.lastParsed)).ifPresent(occ::setLastParsed);
    getDateValue(hit, occurrenceBaseEsFieldMapper.getEsField(GbifTerm.lastCrawled)).ifPresent(occ::setLastCrawled);
  }

  private void parseMultimediaItems(SearchHit hit, Occurrence occ) {

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


    getObjectsListValue(hit, occurrenceBaseEsFieldMapper.getEsField(UnknownTerm.build("multimediaItems")).getValueFieldName())
      .map(i -> i.stream().map(mapFn).collect(Collectors.toList()))
      .ifPresent(occ::setMedia);
  }

}
