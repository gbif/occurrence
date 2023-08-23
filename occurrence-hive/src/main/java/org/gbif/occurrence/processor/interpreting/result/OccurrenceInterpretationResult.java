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
package org.gbif.occurrence.processor.interpreting.result;

import org.gbif.api.model.occurrence.Occurrence;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class OccurrenceInterpretationResult {

  private final Occurrence original;
  private final Occurrence updated;

  public OccurrenceInterpretationResult(@Nullable Occurrence original, Occurrence updated) {
    this.original = original;
    this.updated = checkNotNull(updated, "updated can't be null");
  }

  public Occurrence getOriginal() {
    return original;
  }

  public Occurrence getUpdated() {
    return updated;
  }
}
