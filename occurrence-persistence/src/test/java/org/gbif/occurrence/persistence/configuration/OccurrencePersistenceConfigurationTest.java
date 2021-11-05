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
package org.gbif.occurrence.persistence.configuration;

import org.gbif.api.service.occurrence.OccurrenceService;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled("Not needed for pipelines")
public class OccurrencePersistenceConfigurationTest {

  private final OccurrenceService occurrenceService;

  @Autowired
  public OccurrencePersistenceConfigurationTest(OccurrenceService occurrenceService) {
    this.occurrenceService = occurrenceService;
  }

  // ensure that the service can be instantiated - if you change this, change the README to match!
  @Test
  public void testModule() {
    assertNotNull(occurrenceService);
  }
}
