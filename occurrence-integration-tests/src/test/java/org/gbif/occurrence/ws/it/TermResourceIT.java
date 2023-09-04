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
package org.gbif.occurrence.ws.it;

import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.ws.resources.TermResource;

import java.util.List;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@AutoConfigureMockMvc
@SpringBootTest(
  classes = OccurrenceWsItConfiguration.class,
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TermResourceIT {

  private final TermResource termResource;

  @Autowired
  public TermResourceIT(TermResource termResource) {
    this.termResource = termResource;
  }

  /**
   * Test that all verbatim and interpreted Terms are returned by the termResource.getInterpretation method.
   */
  @Test
  public void testGetInterpretation() {
    List<TermResource.TermWrapper> terms =  termResource.getInterpretation();
    assertNotNull(terms);
    assertTrue(StreamSupport.stream(TermUtils.interpretedTerms().spliterator(), false)
                            .allMatch(t -> terms.stream().anyMatch(tw -> tw.getSimpleName().equals(t.simpleName()))));

    assertTrue(StreamSupport.stream(TermUtils.verbatimTerms().spliterator(), false)
                            .allMatch(t -> terms.stream().anyMatch(tw -> tw.getSimpleName().equals(t.simpleName()))));

  }

}
