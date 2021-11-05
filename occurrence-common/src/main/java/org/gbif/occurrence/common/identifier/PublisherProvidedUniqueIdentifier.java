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

import java.util.UUID;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class PublisherProvidedUniqueIdentifier implements UniqueIdentifier {

  private final UUID datasetKey;
  private final String publisherProvidedIdentifier;


  public PublisherProvidedUniqueIdentifier(UUID datasetKey, String publisherProvidedIdentifier) {
    this.datasetKey = checkNotNull(datasetKey, "datasetKey can't be null");
    this.publisherProvidedIdentifier =
      checkNotNull(publisherProvidedIdentifier, "publisherProvidedIdentifier can't be null");
  }

  public String getPublisherProvidedIdentifier() {
    return publisherProvidedIdentifier;
  }

  @Override
  public UUID getDatasetKey() {
    return datasetKey;
  }

  @Override
  public String getUniqueString() {
    return OccurrenceKeyHelper.buildKey(this);
  }

  @Override
  public String getUnscopedUniqueString() {
    return OccurrenceKeyHelper.buildUnscopedKey(this);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(datasetKey, publisherProvidedIdentifier);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final PublisherProvidedUniqueIdentifier other = (PublisherProvidedUniqueIdentifier) obj;
    return Objects.equal(this.datasetKey, other.datasetKey) && Objects
      .equal(this.publisherProvidedIdentifier, other.publisherProvidedIdentifier);
  }
}
