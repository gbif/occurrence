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
package org.gbif.occurrence.search.writer;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

/**
 * Utility class that encapsulates how the occurrence fields are concatenated to compose a single text field.
 */
public class FullTextFieldBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(FullTextFieldBuilder.class);

  public static final Set<Term> NON_FULL_TEXT_TERMS = new ImmutableSet.Builder<Term>().add(DwcTerm.verbatimEventDate,
                                                                                           DwcTerm.verbatimCoordinates,
                                                                                           DwcTerm.verbatimCoordinateSystem,
                                                                                           DwcTerm.verbatimDepth,
                                                                                           DwcTerm.verbatimElevation,
                                                                                           DwcTerm.verbatimLatitude,
                                                                                           DwcTerm.verbatimLongitude,
                                                                                           DwcTerm.eventTime,
                                                                                           DwcTerm.taxonID,
                                                                                           GbifTerm.gbifID,
                                                                                           GbifTerm.datasetKey).build();

  /**
   * Private hidden constructor.
   */
  private FullTextFieldBuilder() {
    //Utility classes hide constructors
  }

  /**
   * Collects all the values for the full_text field.
   */
  public static Set<String> buildFullTextField(Occurrence occurrence) {
    Set<String> fullTextField = new HashSet<String>();
    try {
      for (Map.Entry<String, Object> properties : PropertyUtils.describe(occurrence).entrySet()) {
        Object value = properties.getValue();
        if (value != null && !nonFullTextTypes(value)) {
          fullTextField.add(value.toString());
        }
      }

      for (Map.Entry<Term,String> verbatimField : occurrence.getVerbatimFields().entrySet()) {
        if (verbatimField.getValue() != null && !NON_FULL_TEXT_TERMS.contains(verbatimField.getKey())) {
          fullTextField.add(verbatimField.getValue());
        }
      }
      if (occurrence.getYear() != null) {
        fullTextField.add(occurrence.getYear().toString());
      }
      if (occurrence.getMonth() != null) {
        fullTextField.add(occurrence.getMonth().toString());
      }
      if (occurrence.getDay() != null) {
        fullTextField.add(occurrence.getDay().toString());
      }
    } catch (Exception ex) {
      LOG.error("Error extracting values for the full_text field", ex);
    }
    return fullTextField;
  }

  /**
   *  Utility function to filter out unsupported types in full text searches.
   */
  private static boolean nonFullTextTypes(Object value) {
    Class<?> type = value.getClass();
    return type.isAssignableFrom(Boolean.class)
           || type.isAssignableFrom(Integer.class)
           || type.isAssignableFrom(Date.class)
           || type.isAssignableFrom(UUID.class)
           || type.isArray()
           || value instanceof Map
           || value instanceof Collection
           || value instanceof EnumSet;
  }
}
