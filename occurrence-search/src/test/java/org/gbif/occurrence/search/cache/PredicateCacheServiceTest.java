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
package org.gbif.occurrence.search.cache;

import org.gbif.api.model.occurrence.predicate.IsNullPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import org.cache2k.config.Cache2kConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for the default PredicateCacheService.*/
public class PredicateCacheServiceTest {

  @Test
  public void putAndGetTest() {
    //Cache config
    Cache2kConfig<Integer,Predicate> cache2kConfig = new Cache2kConfig<>();
    cache2kConfig.setEntryCapacity(1);
    cache2kConfig.setPermitNullValues(true);

    PredicateCacheService predicateCacheService = DefaultInMemoryPredicateCacheService.getInstance(cache2kConfig);
    IsNullPredicate isNullPredicate = new IsNullPredicate(OccurrenceSearchParameter.BASIS_OF_RECORD);

    //Store the predicate
    Integer predicateHash = predicateCacheService.put(isNullPredicate);

    //Compare hashes
    assertEquals(predicateHash, predicateCacheService.hash(isNullPredicate));

    //Compare predicates
    assertEquals(isNullPredicate, predicateCacheService.get(predicateHash));
  }
}
