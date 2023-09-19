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
package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.WithinPredicate;

import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PredicateValidatorTest {


  @Test
  public void invalidPolygonTest() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      //Invalid polygon
      WithinPredicate withinPredicate = new WithinPredicate("POLYGON ((30 10 10))");

      //Valid depth
      EqualsPredicate equalsPredicate = new EqualsPredicate(OccurrenceSearchParameter.DEPTH, "10", true);

      //Conjunction predicate
      ConjunctionPredicate conjunctionPredicate = new ConjunctionPredicate(Arrays.asList(withinPredicate, equalsPredicate));

      //Test
      PredicateValidator.validate(conjunctionPredicate);
    });
  }

  @Test
  public void validPolygonTest() {

      //Valid polygon
      WithinPredicate withinPredicate = new WithinPredicate("POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))");

      //Valid depth
      EqualsPredicate equalsPredicate = new EqualsPredicate(OccurrenceSearchParameter.DEPTH, "10", true);

      //Conjunction predicate
      ConjunctionPredicate conjunctionPredicate = new ConjunctionPredicate(Arrays.asList(withinPredicate, equalsPredicate));

      //Test
      PredicateValidator.validate(conjunctionPredicate);

  }
}
