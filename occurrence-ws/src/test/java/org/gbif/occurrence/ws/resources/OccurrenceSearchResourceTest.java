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
package org.gbif.occurrence.ws.resources;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.google.common.base.CaseFormat;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;

import static org.springframework.test.util.AssertionErrors.assertTrue;

public class OccurrenceSearchResourceTest {

  @Test
  public void searchParametersDocumented() throws Exception {

    Set<String> documentedParameters =
        Arrays.stream(
                OccurrenceSearchResource.class
                    .getMethod("search", OccurrenceSearchRequest.class)
                    .getAnnotation(Parameters.class)
                    .value())
            .map(Parameter::name)
            .collect(Collectors.toSet());

    for (OccurrenceSearchParameter param : OccurrenceSearchParameter.values()) {
      if (param == OccurrenceSearchParameter.EVENT_ID_HIERARCHY
        || param == OccurrenceSearchParameter.EVENT_TYPE
        || param == OccurrenceSearchParameter.VERBATIM_EVENT_TYPE) {
        // skip events-only parameters
        continue;
      }

      String name = null;
      if (param.equals(OccurrenceSearchParameter.IDENTIFIED_BY_ID)) {
        name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, param.name()).replace("Id", "ID");
      } else if (param.equals(OccurrenceSearchParameter.RECORDED_BY_ID)) {
        name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, param.name()).replace("Id", "ID");
      } else {
        name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, param.name());
      }

      assertTrue("Search parameter " + param + "/" + name + " is not documented", documentedParameters.contains(name));
    }
  }
}
