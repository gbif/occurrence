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
package org.gbif.occurrence.search;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.search.es.EsField;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.occurrence.search.es.SearchHitConverter;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.common.Strings;
import org.elasticsearch.search.SearchHit;

import com.google.common.collect.Maps;

import static org.gbif.occurrence.search.es.OccurrenceEsField.CRAWL_ID;
import static org.gbif.occurrence.search.es.OccurrenceEsField.DATASET_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.GBIF_ID;
import static org.gbif.occurrence.search.es.OccurrenceEsField.HOSTING_ORGANIZATION_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.ID;
import static org.gbif.occurrence.search.es.OccurrenceEsField.INSTALLATION_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.LAST_CRAWLED;
import static org.gbif.occurrence.search.es.OccurrenceEsField.LAST_PARSED;
import static org.gbif.occurrence.search.es.OccurrenceEsField.NETWORK_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.PROTOCOL;
import static org.gbif.occurrence.search.es.OccurrenceEsField.PUBLISHING_COUNTRY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.PUBLISHING_ORGANIZATION_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.VERBATIM;

public class VerbatimSearchHitConverter extends SearchHitConverter<VerbatimOccurrence> {

  private final boolean excludeInterpretedFromVerbatim;
  private final EsField verbatimField;

  public VerbatimSearchHitConverter(OccurrenceBaseEsFieldMapper occurrenceBaseEsFieldMapper,
                                    EsField verbatimField,
                                    boolean excludeInterpretedFromVerbatim) {
    super(occurrenceBaseEsFieldMapper);
    this.excludeInterpretedFromVerbatim = excludeInterpretedFromVerbatim;
    this.verbatimField = verbatimField;

  }

  @Override
  public VerbatimOccurrence apply(SearchHit hit) {
    VerbatimOccurrence vOcc = new VerbatimOccurrence();
    getValue(hit, PUBLISHING_COUNTRY, v -> Country.fromIsoCode(v.toUpperCase()))
      .ifPresent(vOcc::setPublishingCountry);
    getValue(hit, DATASET_KEY, UUID::fromString).ifPresent(vOcc::setDatasetKey);
    getValue(hit, INSTALLATION_KEY, UUID::fromString).ifPresent(vOcc::setInstallationKey);
    getValue(hit, PUBLISHING_ORGANIZATION_KEY, UUID::fromString)
      .ifPresent(vOcc::setPublishingOrgKey);
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

    getStringValue(hit, ID)
      .filter(k -> !k.equals(gbifId) && (!Strings.isNullOrEmpty(occId) || !k.equals(triplet)))
      .ifPresent(result -> occ.getVerbatimFields().put(DcTerm.identifier, result));
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
}
