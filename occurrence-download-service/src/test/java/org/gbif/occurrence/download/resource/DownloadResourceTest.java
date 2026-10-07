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
package org.gbif.occurrence.download.resource;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.IsNotNullPredicate;
import org.gbif.api.model.predicate.Predicate;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DownloadResourceTest {

  @Test
  void containsSequenceNotNullPredicateReturnsTrueForExactMatch() {
    Predicate predicate =
        new IsNotNullPredicate<>(OccurrenceSearchParameter.NUCLEOTIDE_SEQUENCE_SEQUENCE);

    assertTrue(DownloadResource.containsSequenceNotNullPredicate(predicate));
  }

  @Test
  void containsSequenceNotNullPredicateReturnsTrueForNestedConjunction() {
    Predicate predicate =
        new ConjunctionPredicate(
            List.of(
                new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "1", false),
                new IsNotNullPredicate<>(OccurrenceSearchParameter.NUCLEOTIDE_SEQUENCE_SEQUENCE)));

    assertTrue(DownloadResource.containsSequenceNotNullPredicate(predicate));

    predicate =
        new ConjunctionPredicate(
            List.of(
                new ConjunctionPredicate(
                    List.of(
                        new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "1", false),
                        new EqualsPredicate<>(OccurrenceSearchParameter.YEAR, "2010", false))),
                new IsNotNullPredicate<>(OccurrenceSearchParameter.NUCLEOTIDE_SEQUENCE_SEQUENCE)));

    assertTrue(DownloadResource.containsSequenceNotNullPredicate(predicate));
  }

  @Test
  void containsSequenceNotNullPredicateReturnsFalseWhenMissing() {
    Predicate predicate =
        new ConjunctionPredicate(
            List.of(
                new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "1", false),
                new EqualsPredicate<>(OccurrenceSearchParameter.COUNTRY, "GB", false)));

    assertFalse(DownloadResource.containsSequenceNotNullPredicate(predicate));
  }
}
