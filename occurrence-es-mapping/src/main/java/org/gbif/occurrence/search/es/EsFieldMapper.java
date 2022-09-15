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

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.common.TermUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.occurrence.search.es.OccurrenceEsField.*;

@Data
@Builder
@Slf4j
public class EsFieldMapper {

  private static final Pattern NESTED_PATTERN = Pattern.compile("^\\w+(\\.\\w+)+$");
  private static final Predicate<String> IS_NESTED = s -> NESTED_PATTERN.matcher(s).find();

  public static final ImmutableMap<OccurrenceSearchParameter, OccurrenceEsField> SEARCH_TO_ES_MAPPING =
      ImmutableMap.<OccurrenceSearchParameter, OccurrenceEsField>builder()
          .put(OccurrenceSearchParameter.DECIMAL_LATITUDE, LATITUDE)
          .put(OccurrenceSearchParameter.DECIMAL_LONGITUDE, LONGITUDE)
          .put(OccurrenceSearchParameter.YEAR, YEAR)
          .put(OccurrenceSearchParameter.MONTH, MONTH)
          .put(OccurrenceSearchParameter.CATALOG_NUMBER, CATALOG_NUMBER)
          .put(OccurrenceSearchParameter.RECORDED_BY, RECORDED_BY)
          .put(OccurrenceSearchParameter.IDENTIFIED_BY, IDENTIFIED_BY)
          .put(OccurrenceSearchParameter.RECORD_NUMBER, RECORD_NUMBER)
          .put(OccurrenceSearchParameter.COLLECTION_CODE, COLLECTION_CODE)
          .put(OccurrenceSearchParameter.INSTITUTION_CODE, INSTITUTION_CODE)
          .put(OccurrenceSearchParameter.DEPTH, DEPTH)
          .put(OccurrenceSearchParameter.ELEVATION, ELEVATION)
          .put(OccurrenceSearchParameter.BASIS_OF_RECORD, BASIS_OF_RECORD)
          .put(OccurrenceSearchParameter.DATASET_KEY, DATASET_KEY)
          .put(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, HAS_GEOSPATIAL_ISSUES)
          .put(OccurrenceSearchParameter.HAS_COORDINATE, HAS_COORDINATE)
          .put(OccurrenceSearchParameter.EVENT_DATE, EVENT_DATE)
          .put(OccurrenceSearchParameter.MODIFIED, MODIFIED)
          .put(OccurrenceSearchParameter.LAST_INTERPRETED, LAST_INTERPRETED)
          .put(OccurrenceSearchParameter.COUNTRY, COUNTRY_CODE)
          .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, PUBLISHING_COUNTRY)
          .put(OccurrenceSearchParameter.CONTINENT, CONTINENT)
          .put(OccurrenceSearchParameter.TAXON_KEY, TAXON_KEY)
          .put(OccurrenceSearchParameter.KINGDOM_KEY, KINGDOM_KEY)
          .put(OccurrenceSearchParameter.PHYLUM_KEY, PHYLUM_KEY)
          .put(OccurrenceSearchParameter.CLASS_KEY, CLASS_KEY)
          .put(OccurrenceSearchParameter.ORDER_KEY, ORDER_KEY)
          .put(OccurrenceSearchParameter.FAMILY_KEY, FAMILY_KEY)
          .put(OccurrenceSearchParameter.GENUS_KEY, GENUS_KEY)
          .put(OccurrenceSearchParameter.SUBGENUS_KEY, SUBGENUS_KEY)
          .put(OccurrenceSearchParameter.SPECIES_KEY, SPECIES_KEY)
          .put(OccurrenceSearchParameter.SCIENTIFIC_NAME, SCIENTIFIC_NAME)
          .put(OccurrenceSearchParameter.VERBATIM_SCIENTIFIC_NAME, VERBATIM_SCIENTIFIC_NAME)
          .put(OccurrenceSearchParameter.TAXON_ID, TAXON_ID)
          .put(OccurrenceSearchParameter.TYPE_STATUS, TYPE_STATUS)
          .put(OccurrenceSearchParameter.MEDIA_TYPE, MEDIA_TYPE)
          .put(OccurrenceSearchParameter.ISSUE, ISSUE)
          .put(OccurrenceSearchParameter.OCCURRENCE_ID, OCCURRENCE_ID)
          .put(OccurrenceSearchParameter.ESTABLISHMENT_MEANS, ESTABLISHMENT_MEANS)
          .put(OccurrenceSearchParameter.DEGREE_OF_ESTABLISHMENT, DEGREE_OF_ESTABLISHMENT_MEANS)
          .put(OccurrenceSearchParameter.PATHWAY, PATHWAY)
          .put(OccurrenceSearchParameter.REPATRIATED, REPATRIATED)
          .put(OccurrenceSearchParameter.LOCALITY, LOCALITY)
          .put(OccurrenceSearchParameter.COORDINATE_UNCERTAINTY_IN_METERS, COORDINATE_UNCERTAINTY_IN_METERS)
          .put(OccurrenceSearchParameter.GADM_GID, GADM_GID)
          .put(OccurrenceSearchParameter.GADM_LEVEL_0_GID, GADM_LEVEL_0_GID)
          .put(OccurrenceSearchParameter.GADM_LEVEL_1_GID, GADM_LEVEL_1_GID)
          .put(OccurrenceSearchParameter.GADM_LEVEL_2_GID, GADM_LEVEL_2_GID)
          .put(OccurrenceSearchParameter.GADM_LEVEL_3_GID, GADM_LEVEL_3_GID)
          .put(OccurrenceSearchParameter.STATE_PROVINCE, STATE_PROVINCE)
          .put(OccurrenceSearchParameter.WATER_BODY, WATER_BODY)
          .put(OccurrenceSearchParameter.LICENSE, LICENSE)
          .put(OccurrenceSearchParameter.PROTOCOL, PROTOCOL)
          .put(OccurrenceSearchParameter.ORGANISM_ID, ORGANISM_ID)
          .put(OccurrenceSearchParameter.PUBLISHING_ORG, PUBLISHING_ORGANIZATION_KEY)
          .put(OccurrenceSearchParameter.HOSTING_ORGANIZATION_KEY, HOSTING_ORGANIZATION_KEY)
          .put(OccurrenceSearchParameter.CRAWL_ID, CRAWL_ID)
          .put(OccurrenceSearchParameter.INSTALLATION_KEY, INSTALLATION_KEY)
          .put(OccurrenceSearchParameter.NETWORK_KEY, NETWORK_KEY)
          .put(OccurrenceSearchParameter.EVENT_ID, EVENT_ID)
          .put(OccurrenceSearchParameter.PARENT_EVENT_ID, PARENT_EVENT_ID)
          .put(OccurrenceSearchParameter.SAMPLING_PROTOCOL, SAMPLING_PROTOCOL)
          .put(OccurrenceSearchParameter.PROJECT_ID, PROJECT_ID)
          .put(OccurrenceSearchParameter.PROGRAMME, PROGRAMME)
          .put(OccurrenceSearchParameter.ORGANISM_QUANTITY, ORGANISM_QUANTITY)
          .put(OccurrenceSearchParameter.ORGANISM_QUANTITY_TYPE, ORGANISM_QUANTITY_TYPE)
          .put(OccurrenceSearchParameter.SAMPLE_SIZE_VALUE, SAMPLE_SIZE_VALUE)
          .put(OccurrenceSearchParameter.SAMPLE_SIZE_UNIT, SAMPLE_SIZE_UNIT)
          .put(OccurrenceSearchParameter.RELATIVE_ORGANISM_QUANTITY, RELATIVE_ORGANISM_QUANTITY)
          .put(OccurrenceSearchParameter.COLLECTION_KEY, COLLECTION_KEY)
          .put(OccurrenceSearchParameter.INSTITUTION_KEY, INSTITUTION_KEY)
          .put(OccurrenceSearchParameter.IDENTIFIED_BY_ID, IDENTIFIED_BY_ID_VALUE)
          .put(OccurrenceSearchParameter.RECORDED_BY_ID, RECORDED_BY_ID_VALUE)
          .put(OccurrenceSearchParameter.OCCURRENCE_STATUS, OCCURRENCE_STATUS)
          .put(OccurrenceSearchParameter.LIFE_STAGE, LIFE_STAGE)
          .put(OccurrenceSearchParameter.IS_IN_CLUSTER, IS_IN_CLUSTER)
          .put(OccurrenceSearchParameter.DWCA_EXTENSION, EXTENSIONS)
          .put(OccurrenceSearchParameter.IUCN_RED_LIST_CATEGORY, IUCN_RED_LIST_CATEGORY)
          .put(OccurrenceSearchParameter.DATASET_ID, DATASET_ID)
          .put(OccurrenceSearchParameter.DATASET_NAME, DATASET_NAME)
          .put(OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS, OTHER_CATALOG_NUMBERS)
          .put(OccurrenceSearchParameter.PREPARATIONS, PREPARATIONS)
          .build();

  static final Map<String, OccurrenceSearchParameter> ES_TO_SEARCH_MAPPING =
    new HashMap<>(EsFieldMapper.SEARCH_TO_ES_MAPPING.size());

  static {
    for (Map.Entry<OccurrenceSearchParameter, OccurrenceEsField> paramField :
      EsFieldMapper.SEARCH_TO_ES_MAPPING.entrySet()) {
      ES_TO_SEARCH_MAPPING.put(paramField.getValue().getSearchFieldName(), paramField.getKey());
    }
  }

  static final Set<OccurrenceEsField> VOCABULARY_FIELDS = Arrays.stream(OccurrenceEsField.values())
    .filter(f -> TermUtils.isVocabulary(f.getTerm()))
    .collect(Collectors.toSet());

  public enum SearchType {
    OCCURRENCE("occurrence"), EVENT("event"), METADATA("metadata");

    private String objectName;

    public String getObjectName() {
      return objectName;
    }

    SearchType(String objectName) {
      this.objectName = objectName;
    }
  }

  private final static Set<OccurrenceEsField> METADATA_FIELDS = new HashSet<>(Arrays.asList(DATASET_KEY,
                                                                                            PUBLISHING_ORGANIZATION_KEY,
                                                                                            HOSTING_ORGANIZATION_KEY,
                                                                                            INSTALLATION_KEY,
                                                                                            NETWORK_KEY,
                                                                                            PROTOCOL,
                                                                                            PROJECT_ID,
                                                                                            PROGRAMME));

  private final static Set<OccurrenceEsField> ROOT_LEVEL_FIELDS =  new HashSet<>(Arrays.asList(ID,
                                                                                               FULL_TEXT,
                                                                                               LAST_CRAWLED,
                                                                                               CRAWL_ID,
                                                                                               LAST_INTERPRETED,
                                                                                               LAST_PARSED,
                                                                                               VERBATIM));
  private SearchType searchType;

  private final boolean nestedIndex;

  public String getFieldName(OccurrenceEsField occurrenceEsField, String fieldName) {
    if (!nestedIndex || ROOT_LEVEL_FIELDS.contains(occurrenceEsField)) {
      return fieldName;
    }
    if (METADATA_FIELDS.contains(occurrenceEsField)) {
      return SearchType.METADATA.getObjectName() + '.' + fieldName;
    }
    return searchType.getObjectName() + '.' + fieldName;
  }

  public OccurrenceSearchParameter getSearchParameter(String searchFieldName) {
    return ES_TO_SEARCH_MAPPING.get(searchFieldName);
  }

  public String getSearchFieldName(String fieldName) {
    return nestedIndex? searchType.getObjectName() + '.' + fieldName : fieldName;
  }

  public String getSearchFieldName(OccurrenceEsField occurrenceEsField) {
    return getFieldName(occurrenceEsField, occurrenceEsField.getSearchFieldName());
  }

  public String getSearchFieldName(OccurrenceSearchParameter searchParameter) {
    OccurrenceEsField occurrenceEsField = getOccurrenceEsField(searchParameter);
    return getFieldName(occurrenceEsField, occurrenceEsField.getSearchFieldName());
  }

  public String getExactMatchFieldName(OccurrenceEsField occurrenceEsField) {
    return getFieldName(occurrenceEsField, occurrenceEsField.getExactMatchFieldName());
  }

  public String getExactMatchFieldName(OccurrenceSearchParameter searchParameter) {
    OccurrenceEsField occurrenceEsField = getOccurrenceEsField(searchParameter);
    return getFieldName(occurrenceEsField, occurrenceEsField.getExactMatchFieldName());
  }

  public String getVerbatimFieldName(OccurrenceEsField occurrenceEsField) {
    return getFieldName(occurrenceEsField, occurrenceEsField.getVerbatimFieldName());
  }

  public String getVerbatimFieldName(OccurrenceSearchParameter searchParameter) {
    OccurrenceEsField occurrenceEsField = getOccurrenceEsField(searchParameter);
    return getFieldName(occurrenceEsField, occurrenceEsField.getVerbatimFieldName());
  }

  public String getValueFieldName(OccurrenceSearchParameter searchParameter) {
    OccurrenceEsField occurrenceEsField = getOccurrenceEsField(searchParameter);
    return getFieldName(occurrenceEsField, occurrenceEsField.getValueFieldName());
  }

  public String getSuggestFieldName(OccurrenceEsField occurrenceEsField) {
    return getFieldName(occurrenceEsField, occurrenceEsField.getSuggestFieldName());
  }

  public OccurrenceEsField getOccurrenceEsField(OccurrenceSearchParameter searchParameter) {
    return SEARCH_TO_ES_MAPPING.get(searchParameter);
  }

  public static boolean isVocabulary(OccurrenceEsField esField) {
    return VOCABULARY_FIELDS.contains(esField);
  }


}
