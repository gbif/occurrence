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

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.NotPredicate;
import org.gbif.api.model.predicate.Predicate;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestLimitsTest {

  private final RequestLimits limits = new RequestLimits(3, 5, 6, 3, 2, RequestLimits.SEARCH_HINT);

  private static List<String> values(int n) {
    return IntStream.range(0, n).mapToObj(String::valueOf).collect(Collectors.toList());
  }

  private static EqualsPredicate<OccurrenceSearchParameter> eq(OccurrenceSearchParameter p, String v) {
    return new EqualsPredicate<>(p, v, false);
  }

  @Test
  void parametersWithinLimits() {
    Map<OccurrenceSearchParameter, Set<String>> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.YEAR, Set.copyOf(values(3)));
    params.put(OccurrenceSearchParameter.COUNTRY, Set.of("DK", "FR"));
    assertDoesNotThrow(() -> limits.validate(params));
  }

  @Test
  void tooManyValuesForOneParameter() {
    Map<OccurrenceSearchParameter, Set<String>> params =
        Map.of(OccurrenceSearchParameter.TAXON_KEY, Set.copyOf(values(4)));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> limits.validate(params));
    assertTrue(e.getMessage().contains("TAXON_KEY"));
    assertTrue(e.getMessage().contains("download API"));
  }

  @Test
  void tooManyValuesInTotal() {
    Map<OccurrenceSearchParameter, Set<String>> params = new HashMap<>();
    params.put(OccurrenceSearchParameter.YEAR, Set.copyOf(values(3)));
    params.put(OccurrenceSearchParameter.MONTH, Set.copyOf(values(3)));
    assertThrows(IllegalArgumentException.class, () -> limits.validate(params));
  }

  @Test
  void predicateInValuesCountAsLeavesAndPerParameter() {
    Predicate in = new InPredicate<>(OccurrenceSearchParameter.TAXON_KEY, values(4), false);
    assertThrows(IllegalArgumentException.class, () -> limits.validate(in));

    // the same key spread across several leaves is summed
    Predicate and =
        new ConjunctionPredicate(
            List.of(
                new InPredicate<>(OccurrenceSearchParameter.TAXON_KEY, values(2), false),
                eq(OccurrenceSearchParameter.TAXON_KEY, "9")));
    assertDoesNotThrow(() -> limits.validate(and));
  }

  @Test
  void tooManyPredicateLeaves() {
    Predicate and =
        new ConjunctionPredicate(
            List.of(
                new InPredicate<>(OccurrenceSearchParameter.YEAR, values(3), false),
                new InPredicate<>(OccurrenceSearchParameter.CATALOG_NUMBER, values(3), false),
                eq(OccurrenceSearchParameter.COUNTRY, "DK")));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> limits.validate(and));
    assertTrue(e.getMessage().contains("leaf"));
  }

  @Test
  void predicateTooDeep() {
    Predicate p = eq(OccurrenceSearchParameter.COUNTRY, "DK");
    // depth 3 is fine
    Predicate ok = new NotPredicate(new ConjunctionPredicate(Collections.singletonList(p)));
    assertDoesNotThrow(() -> limits.validate(ok));

    Predicate deep = new ConjunctionPredicate(Collections.singletonList(ok));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> limits.validate(deep));
    assertTrue(e.getMessage().contains("nested"));
  }

  @Test
  void tooManySameKeySiblings() {
    Predicate or =
        new DisjunctionPredicate(
            List.of(
                eq(OccurrenceSearchParameter.TAXON_KEY, "1"),
                eq(OccurrenceSearchParameter.TAXON_KEY, "2"),
                eq(OccurrenceSearchParameter.TAXON_KEY, "3")));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> limits.validate(or));
    assertTrue(e.getMessage().contains("'in'"));
  }

  @Test
  void commaInKeyValue() {
    assertThrows(
        IllegalArgumentException.class, () -> limits.validate(eq(OccurrenceSearchParameter.TAXON_KEY, "5204135,")));
    assertThrows(
        IllegalArgumentException.class,
        () -> limits.validate(new InPredicate<>(OccurrenceSearchParameter.TAXON_KEY, List.of("1", "2,3"), false)));
    // commas are fine in free-text parameters
    assertDoesNotThrow(() -> limits.validate(eq(OccurrenceSearchParameter.LOCALITY, "Copenhagen, Denmark")));
  }

  /** Shape of download 0023281-261023074918325: one 'and' with 100,000 TAXON_KEY equals, values like "5204135,". */
  @Test
  void rejectsDownload0023281() {
    RequestLimits downloadDefaults = new RequestLimits(100_000, Integer.MAX_VALUE, Integer.MAX_VALUE, 20, 1000, "");
    List<Predicate> leaves =
        IntStream.range(0, 100_000)
            .mapToObj(i -> (Predicate) eq(OccurrenceSearchParameter.TAXON_KEY, (i % 12_609) + ","))
            .collect(Collectors.toList());
    assertThrows(IllegalArgumentException.class, () -> downloadDefaults.validate(new ConjunctionPredicate(leaves)));

    // still rejected with clean values, by the same-key sibling cap
    List<Predicate> clean =
        IntStream.range(0, 100_000)
            .mapToObj(i -> (Predicate) eq(OccurrenceSearchParameter.TAXON_KEY, String.valueOf(i % 12_609)))
            .collect(Collectors.toList());
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> downloadDefaults.validate(new ConjunctionPredicate(clean)));
    assertTrue(e.getMessage().contains("TAXON_KEY"));

    // whereas the same keys as one 'in' are accepted
    Predicate in =
        new InPredicate<>(
            OccurrenceSearchParameter.TAXON_KEY,
            IntStream.range(0, 12_609).mapToObj(String::valueOf).collect(Collectors.toList()),
            false);
    assertDoesNotThrow(() -> downloadDefaults.validate(in));
  }

  @Test
  void nullsAreAccepted() {
    assertDoesNotThrow(() -> limits.validate((Predicate) null));
    assertDoesNotThrow(() -> limits.validate((Map<OccurrenceSearchParameter, Set<String>>) null));
  }
}
