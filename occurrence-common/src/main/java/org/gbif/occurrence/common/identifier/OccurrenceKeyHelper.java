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
package org.gbif.occurrence.common.identifier;

import javax.annotation.Nullable;

/**
 * A helper class for building the row keys used in the occurrence lookup table/process.
 * TODO: this is too similar to OccurrenceKeyBuilder - they should be merged
 */
public class OccurrenceKeyHelper {

  private static final char DELIM = '|';

  private OccurrenceKeyHelper() {
  }

  public static String buildKeyPrefix(String datasetKey) {
    return datasetKey + DELIM;
  }

  @Nullable
  public static String buildKey(@Nullable HolyTriplet triplet) {
    if (triplet == null || triplet.getDatasetKey() == null || triplet.getInstitutionCode() == null
        || triplet.getCollectionCode() == null || triplet.getCatalogNumber() == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder(triplet.getDatasetKey().toString());
    sb.append(DELIM);
    sb.append(triplet.getInstitutionCode());
    sb.append(DELIM);
    sb.append(triplet.getCollectionCode());
    sb.append(DELIM);
    sb.append(triplet.getCatalogNumber());
    sb.append(DELIM);
    sb.append(triplet.getUnitQualifier());

    return sb.toString();
  }

  @Nullable
  public static String buildKey(@Nullable PublisherProvidedUniqueIdentifier pubProvided) {
    if (pubProvided == null || pubProvided.getDatasetKey() == null
        || pubProvided.getPublisherProvidedIdentifier() == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder(pubProvided.getDatasetKey().toString());
    sb.append(DELIM);
    sb.append(pubProvided.getPublisherProvidedIdentifier());

    return sb.toString();
  }

  @Nullable
  public static String buildUnscopedKey(@Nullable PublisherProvidedUniqueIdentifier pubProvidedUniqueId) {
    if (pubProvidedUniqueId == null) {
      return null;
    }

    return pubProvidedUniqueId.getPublisherProvidedIdentifier();
  }

  @Nullable
  public static String buildUnscopedKey(@Nullable HolyTriplet triplet) {
    if (triplet == null || triplet.getDatasetKey() == null || triplet.getInstitutionCode() == null
        || triplet.getCollectionCode() == null || triplet.getCatalogNumber() == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder(triplet.getInstitutionCode());
    sb.append(DELIM);
    sb.append(triplet.getCollectionCode());
    sb.append(DELIM);
    sb.append(triplet.getCatalogNumber());
    sb.append(DELIM);
    sb.append(triplet.getUnitQualifier());

    return sb.toString();
  }
}
