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
package org.gbif.occurrence.download.hive;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import lombok.experimental.UtilityClass;

import static org.gbif.occurrence.download.hive.HiveColumns.cleanDelimitersArrayInitializer;
import static org.gbif.occurrence.download.hive.HiveColumns.cleanDelimitersInitializer;
import static org.gbif.occurrence.download.hive.HiveColumns.columnFor;
import static org.gbif.occurrence.download.hive.HiveColumns.getVerbatimColPrefix;

/**
 * This provides the definition required to construct the occurrence hdfs table, for use as a Hive table.
 * The table is populated by a query which scans the Avro files, but along the way converts some fields to
 * e.g. Hive arrays which requires some UDF voodoo captured here.
 * <p/>
 * Note to developers: It is not easy to find a perfectly clean solution to this work.  Here we try and favour long
 * term code management over all else.  For that reason, Functional programming idioms are not used even though they
 * would reduce lines of code.  However, they come at a cost in that there are several levels of method chaining
 * here, and it becomes difficult to follow as they are not so intuitive on first glance.  Similarly, we don't attempt
 * to push all complexities into the freemarker templates (e.g. complex UDF chaining) as it becomes unmanageable.
 * <p/>
 * Developers please adhere to the above design goals when modifying this class, and consider developing for simple
 * maintenance.
 */
@UtilityClass
public class OccurrenceHDFSTableDefinition {

  private static final Set<Term> ARRAYS_FROM_VERBATIM_VALUES =
      ImmutableSet.of(
          DwcTerm.recordedByID,
          DwcTerm.identifiedByID,
          DwcTerm.datasetID,
          DwcTerm.datasetName,
          DwcTerm.recordedBy,
          DwcTerm.identifiedBy,
          DwcTerm.otherCatalogNumbers,
          DwcTerm.preparations,
          DwcTerm.samplingProtocol,
          DwcTerm.higherGeography,
          DwcTerm.georeferencedBy);

  /**
   * Assemble the mapping for verbatim fields.
   *
   * @return the list of fields that are used in the verbatim context
   */
  private static List<InitializableField> verbatimFields() {
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (Term t : DownloadTerms.DOWNLOAD_VERBATIM_TERMS) {
      builder.add(verbatimField(t));
    }
    return builder.build();
  }

  /**
   * Assemble the mapping for interpreted fields, taking note that in reality, many are mounted onto the verbatim
   * columns.
   *
   * @return the list of fields that are used in the interpreted context
   */
  private static List<InitializableField> interpretedFields() {

    // the following terms are manipulated when transposing from Avro to hive by using UDFs and custom HQL
    Map<Term, String> initializers = ImmutableMap.<Term, String>builder()
                                      .put(GbifTerm.datasetKey, columnFor(GbifTerm.datasetKey))
                                      .put(GbifTerm.protocol, columnFor(GbifTerm.protocol))
                                      .put(GbifTerm.publishingCountry, columnFor(GbifTerm.publishingCountry))
                                      .put(DwcTerm.eventType, columnFor(DwcTerm.eventType))
                                      .put(IucnTerm.iucnRedListCategory, columnFor(IucnTerm.iucnRedListCategory))
                                      .build();

    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (Term t : DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS) {
      // if there is custom handling registered for the term, use it
      if (initializers.containsKey(t)) {
        builder.add(interpretedField(t, initializers.get(t)));
      } else {
        builder.add(interpretedField(t)); // will just use the term name as usual
      }

    }
    return builder.build();
  }

  /**
   * The internal fields stored in Avro which we wish to expose through Hive.  The fragment and fragment hash
   * are removed and not present.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<InitializableField> internalFields() {
    Map<Term, String> initializers = new ImmutableMap.Builder<Term,String>()
                                                      .put(GbifInternalTerm.publishingOrgKey, columnFor(GbifInternalTerm.publishingOrgKey))
                                                      .put(GbifInternalTerm.installationKey, columnFor(GbifInternalTerm.installationKey))
                                                      .put(GbifInternalTerm.institutionKey, columnFor(GbifInternalTerm.institutionKey))
                                                      .put(GbifInternalTerm.collectionKey, columnFor(GbifInternalTerm.collectionKey))
                                                      .put(GbifTerm.projectId, columnFor(GbifTerm.projectId))
                                                      .put(GbifInternalTerm.programmeAcronym, columnFor(GbifInternalTerm.programmeAcronym))
                                                      .put(GbifInternalTerm.hostingOrganizationKey, columnFor(GbifInternalTerm.hostingOrganizationKey))
                                                      .put(GbifInternalTerm.isInCluster, columnFor(GbifInternalTerm.isInCluster))
                                                      .put(GbifInternalTerm.dwcaExtension, columnFor(GbifInternalTerm.dwcaExtension))
                                            .build();
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (GbifInternalTerm t : GbifInternalTerm.values()) {
      if (!DownloadTerms.EXCLUSIONS.contains(t)) {
        if (initializers.containsKey(t)) {
          builder.add(interpretedField(t, initializers.get(t)));
        } else {
          builder.add(interpretedField(t));
        }
      }
    }
    return builder.build();
  }

  /**
   * The fields stored in Avro which represent an extension.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<InitializableField> extensions() {
    // only MULTIMEDIA is supported, but coded for future use
    Set<Extension> extensions = ImmutableSet.of(Extension.MULTIMEDIA);
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (Extension e : extensions) {
      builder.add(new InitializableField(GbifTerm.Multimedia,
                                         columnFor(e),
                                         HiveDataTypes.TYPE_STRING
                                         //always, as it has a custom serialization
                  ));
    }
    return builder.build();
  }

  /**
   * Generates the conceptual definition for the occurrence tables when used in hive.
   *
   * @return a list of fields, with the types.
   */
  public static List<InitializableField> definition() {
    return ImmutableList.<InitializableField>builder()
      .add(keyField())
      .addAll(verbatimFields())
      .addAll(internalFields())
      .addAll(interpretedFields())
      .addAll(extensions())
      .build();
  }

  /**
   * Constructs the field for the primary key, which is a special case in that it needs a special mapping.
   */
  private static InitializableField keyField() {
    return new InitializableField(GbifTerm.gbifID,
                                  columnFor(GbifTerm.gbifID),
                                  HiveDataTypes.typeForTerm(GbifTerm.gbifID, true)
                                  // verbatim context
    );
  }

  /**
   * Constructs a Field for the given term, when used in the verbatim context.
   */
  private static InitializableField verbatimField(Term term) {
    String column = getVerbatimColPrefix() + term.simpleName().toLowerCase();
    return new InitializableField(term, column,
                                  // no escape needed, due to prefix
                                  HiveDataTypes.typeForTerm(term, true), // verbatim context
                                  cleanDelimitersInitializer(column) //remove delimiters '\n', '\t', etc.
    );
  }

  /**
   * Constructs a Field for the given term, when used in the interpreted context constructed with no custom
   * initializer.
   */
  private static InitializableField interpretedField(Term term) {
    if (HiveDataTypes.TYPE_STRING.equals(HiveDataTypes.typeForTerm(term, false))) {
      return interpretedField(term, cleanDelimitersInitializer(term)); // no initializer
    }
    if (HiveDataTypes.TYPE_ARRAY_STRING.equals(HiveDataTypes.typeForTerm(term, false))
        && ARRAYS_FROM_VERBATIM_VALUES.contains(term)) {
      return interpretedField(term, cleanDelimitersArrayInitializer(term)); // no initializer
    }

    return interpretedField(term, null); // no initializer
  }

  /**
   * Constructs a Field for the given term, when used in the interpreted context, and setting it up with the
   * given initializer.
   */
  private static InitializableField interpretedField(Term term, String initializer) {
    return new InitializableField(term,
                                  columnFor(term),
                                  // note that Columns takes care of whether this is mounted
                                  // on a verbatim or an interpreted column for us
                                  HiveDataTypes.typeForTerm(term, false),
                                  // not verbatim context
                                  initializer);
  }
}
