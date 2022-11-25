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

import org.gbif.occurrence.search.es.OccurrenceEsField;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import lombok.experimental.UtilityClass;

import static org.gbif.occurrence.search.es.OccurrenceEsField.CLASS_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.FAMILY_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.GENUS_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.KINGDOM_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.ORDER_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.PHYLUM_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.SCIENTIFIC_NAME;
import static org.gbif.occurrence.search.es.OccurrenceEsField.SPECIES_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.SUBGENUS_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.TAXON_ID;
import static org.gbif.occurrence.search.es.OccurrenceEsField.TAXON_KEY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.TYPE_STATUS;
import static org.gbif.occurrence.search.es.OccurrenceEsField.VERBATIM_SCIENTIFIC_NAME;

@UtilityClass
public class ChildrenFieldsMapping {

  private static final String OCCURRENCE_RELATION = "occurrence";

  private static final ImmutableMap<OccurrenceEsField,String> CHILDREN_FIELDS_MAPPING =
    ImmutableMap.<OccurrenceEsField,String>builder()
      .put(TAXON_KEY, OCCURRENCE_RELATION)
      .put(KINGDOM_KEY, OCCURRENCE_RELATION)
      .put(PHYLUM_KEY, OCCURRENCE_RELATION)
      .put(CLASS_KEY, OCCURRENCE_RELATION)
      .put(ORDER_KEY, OCCURRENCE_RELATION)
      .put(FAMILY_KEY, OCCURRENCE_RELATION)
      .put(GENUS_KEY, OCCURRENCE_RELATION)
      .put(SUBGENUS_KEY, OCCURRENCE_RELATION)
      .put(SPECIES_KEY, OCCURRENCE_RELATION)
      .put(SCIENTIFIC_NAME, OCCURRENCE_RELATION)
      .put(VERBATIM_SCIENTIFIC_NAME, OCCURRENCE_RELATION)
      .put(TAXON_ID, OCCURRENCE_RELATION)
      .put(TYPE_STATUS, OCCURRENCE_RELATION)
      .build();

  public static Map<OccurrenceEsField,String> childrenFieldsMappings() {
    return CHILDREN_FIELDS_MAPPING;
  }
}
