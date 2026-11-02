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

import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.predicate.CompoundPredicate;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.IsNotNullPredicate;
import org.gbif.api.model.predicate.IsNullPredicate;
import org.gbif.api.model.predicate.NotPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.model.predicate.RangePredicate;
import org.gbif.api.model.predicate.SimplePredicate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Caps the size of search and download requests.
 *
 * <p>Parameter-style requests are limited by the number of values per parameter and in total. Predicate-style requests
 * are additionally limited by nesting depth and by the number of leaf nodes, where each value of an {@code in}
 * predicate counts as a leaf. Within a single {@code and}/{@code or}, the number of non-{@code in} leaves on the same
 * parameter is capped too (they should be sent as one {@code in}), and values of {@code *_KEY} parameters may not
 * contain commas (a badly joined list such as {@code "5204135,"}). Violations throw an {@link IllegalArgumentException}, which is mapped to HTTP 400.
 */
public class RequestLimits {

  public static final String SEARCH_HINT =
      " For large queries use the occurrence download API instead: https://techdocs.gbif.org/en/data-use/api-downloads";

  private final int maxValuesPerParameter;
  private final int maxValuesTotal;
  private final int maxPredicateLeaves;
  private final int maxPredicateDepth;
  private final int maxSameKeySiblings;
  private final String hint;

  /**
   * @param hint appended to every error message, e.g. {@link #SEARCH_HINT}; may be empty
   */
  public RequestLimits(
      int maxValuesPerParameter,
      int maxValuesTotal,
      int maxPredicateLeaves,
      int maxPredicateDepth,
      int maxSameKeySiblings,
      String hint) {
    this.maxValuesPerParameter = maxValuesPerParameter;
    this.maxValuesTotal = maxValuesTotal;
    this.maxPredicateLeaves = maxPredicateLeaves;
    this.maxPredicateDepth = maxPredicateDepth;
    this.maxSameKeySiblings = maxSameKeySiblings;
    this.hint = Objects.toString(hint, "");
  }

  /** Validates a parameter map as used by the GET/POST search requests. */
  public void validate(Map<? extends SearchParameter, ? extends Collection<String>> parameters) {
    if (parameters == null || parameters.isEmpty()) {
      return;
    }
    Map<String, Integer> counts = new HashMap<>();
    parameters.forEach((p, values) -> counts.put(p.name(), values == null ? 0 : values.size()));
    checkValueCounts(counts);
  }

  /** Validates a predicate tree as used by predicate searches and downloads. */
  public void validate(Predicate predicate) {
    if (predicate == null) {
      return;
    }
    Stats stats = new Stats();
    walk(predicate, 1, stats);
    if (stats.maxDepth > maxPredicateDepth) {
      throw new IllegalArgumentException(
          "The predicate is nested " + stats.maxDepth + " levels deep. The maximum is " + maxPredicateDepth + "."
              + hint);
    }
    if (stats.leaves > maxPredicateLeaves) {
      throw new IllegalArgumentException(
          "The predicate contains " + stats.leaves + " leaf conditions. The maximum is " + maxPredicateLeaves + "."
              + hint);
    }
    checkValueCounts(stats.valuesPerParameter);
  }

  private void checkValueCounts(Map<String, Integer> counts) {
    int total = 0;
    for (Map.Entry<String, Integer> e : counts.entrySet()) {
      if (e.getValue() > maxValuesPerParameter) {
        throw new IllegalArgumentException(
            "Parameter " + e.getKey() + " has " + e.getValue() + " values. The maximum per parameter is "
                + maxValuesPerParameter + "." + hint);
      }
      total += e.getValue();
    }
    if (total > maxValuesTotal) {
      throw new IllegalArgumentException(
          "The request has " + total + " parameter values. The maximum per request is " + maxValuesTotal + "." + hint);
    }
  }

  private void walk(Predicate predicate, int depth, Stats stats) {
    stats.maxDepth = Math.max(stats.maxDepth, depth);
    // Stop early on pathological trees so we don't walk them fully (or overflow the stack).
    if (stats.maxDepth > maxPredicateDepth) {
      return;
    }
    if (predicate instanceof CompoundPredicate compound) {
      if (compound.getPredicates() != null) {
        checkSameKeySiblings(compound);
        for (Predicate p : compound.getPredicates()) {
          walk(p, depth + 1, stats);
        }
      }
    } else if (predicate instanceof NotPredicate not) {
      if (not.getPredicate() != null) {
        walk(not.getPredicate(), depth + 1, stats);
      }
    } else if (predicate instanceof InPredicate<?> in) {
      int n = in.getValues() == null ? 0 : in.getValues().size();
      if (in.getValues() != null) {
        in.getValues().forEach(v -> checkValue(in.getKey(), v));
      }
      stats.leaves += n;
      stats.addValues(in.getKey(), n);
    } else {
      if (predicate instanceof SimplePredicate<?> simple) {
        checkValue(simple.getKey(), simple.getValue());
      }
      stats.leaves++;
      stats.addValues(keyOf(predicate), 1);
    }
  }

  private void checkSameKeySiblings(CompoundPredicate compound) {
    Map<String, Integer> counts = new HashMap<>();
    for (Predicate p : compound.getPredicates()) {
      SearchParameter key = p instanceof InPredicate ? null : keyOf(p);
      if (key != null && counts.merge(key.name(), 1, Integer::sum) > maxSameKeySiblings) {
        String type = compound instanceof DisjunctionPredicate ? "or" : "and";
        throw new IllegalArgumentException(
            "The predicate has more than " + maxSameKeySiblings + " conditions on " + key.name() + " inside one '"
                + type + "'. Send multiple values as a single 'in' predicate instead." + hint);
      }
    }
  }

  private void checkValue(SearchParameter key, String value) {
    if (key != null && value != null && key.name().endsWith("_KEY") && value.indexOf(',') >= 0) {
      throw new IllegalArgumentException(
          "Value '" + value + "' for " + key.name() + " contains a comma. Send multiple values as the 'values' list "
              + "of an 'in' predicate instead.");
    }
  }

  private static SearchParameter keyOf(Predicate predicate) {
    if (predicate instanceof SimplePredicate<?> simple) {
      return simple.getKey();
    } else if (predicate instanceof RangePredicate<?> range) {
      return range.getKey();
    } else if (predicate instanceof IsNullPredicate<?> isNull) {
      return isNull.getParameter();
    } else if (predicate instanceof IsNotNullPredicate<?> isNotNull) {
      return isNotNull.getParameter();
    }
    // within, geoDistance and fullText have no parameter key; they still count as leaves
    return null;
  }

  private static class Stats {
    int maxDepth;
    int leaves;
    final Map<String, Integer> valuesPerParameter = new HashMap<>();

    void addValues(SearchParameter key, int n) {
      if (key != null) {
        valuesPerParameter.merge(key.name(), n, Integer::sum);
      }
    }
  }
}
