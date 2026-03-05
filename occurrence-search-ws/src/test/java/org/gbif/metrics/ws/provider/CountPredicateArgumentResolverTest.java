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
package org.gbif.metrics.ws.provider;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.Predicate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.context.request.NativeWebRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.when;

/**
 * Tests that the argument resolver unpacks HTTP params and builds the expected Predicate
 * for the occurrence count API.
 */
@ExtendWith(MockitoExtension.class)
class CountPredicateArgumentResolverTest {

  private static final UUID DATASET_KEY = UUID.randomUUID();

  @Mock
  private NativeWebRequest webRequestMock;

  private static Predicate expectedPredicate() {
    return new ConjunctionPredicate(
        List.of(
            new EqualsPredicate<>(OccurrenceSearchParameter.BASIS_OF_RECORD, "OBSERVATION", false),
            new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, "212", false),
            new EqualsPredicate<>(OccurrenceSearchParameter.DATASET_KEY, DATASET_KEY.toString(), false)));
  }

  @Test
  void testResolveArgumentBuildsPredicate() {
    CountPredicateArgumentResolver resolver = new CountPredicateArgumentResolver();
    Map<String, String[]> parameters = new LinkedHashMap<>();
    parameters.put("basisOfRecord", new String[] {"OBSERVATION"});
    parameters.put("taxonKey", new String[] {"212"});
    parameters.put("datasetKey", new String[] {DATASET_KEY.toString()});

    when(webRequestMock.getParameterMap()).thenReturn(parameters);

    Predicate result =
        (Predicate) resolver.resolveArgument(null, null, webRequestMock, null);

    assertEquals(expectedPredicate(), result);
  }

  @Test
  void testResolveArgumentSkipsUnknownParams() {
    CountPredicateArgumentResolver resolver = new CountPredicateArgumentResolver();
    Map<String, String[]> parameters = new LinkedHashMap<>();
    parameters.put("basisOfRecord", new String[] {"OBSERVATION"});
    parameters.put("unknownParam", new String[] {"ignored"});
    parameters.put("taxonKey", new String[] {"212"});

    when(webRequestMock.getParameterMap()).thenReturn(parameters);

    Predicate result =
        (Predicate) resolver.resolveArgument(null, null, webRequestMock, null);

    assertInstanceOf(ConjunctionPredicate.class, result);
    assertEquals(2, ((ConjunctionPredicate) result).getPredicates().size());
  }

  @Test
  void testResolveArgumentEmptyParamsReturnsNull() {
    CountPredicateArgumentResolver resolver = new CountPredicateArgumentResolver();
    when(webRequestMock.getParameterMap()).thenReturn(new LinkedHashMap<>());

    Object result = resolver.resolveArgument(null, null, webRequestMock, null);

    assertEquals(null, result);
  }
}
