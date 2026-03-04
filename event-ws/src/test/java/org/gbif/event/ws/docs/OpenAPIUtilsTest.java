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
package org.gbif.event.ws.docs;

import static org.springframework.test.util.AssertionErrors.assertTrue;

import com.google.common.base.CaseFormat;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.junit.jupiter.api.Test;

public class OpenAPIUtilsTest {

  @Test
  public void searchParametersDocumented() throws Exception {

    Set<String> documentedParameters =
        Arrays.stream(OpenAPIUtils.class.getDeclaredClasses())
            .filter(c -> c.getSimpleName().equals("EventSearchParameters"))
            .flatMap(c -> Arrays.stream(c.getAnnotation(Parameters.class).value()))
            .map(Parameter::name)
            .collect(Collectors.toSet());

    for (EventSearchParameter param : EventSearchParameter.values()) {
      // TODO: check these 2 params
      if (param == EventSearchParameter.EVENT_ID_HIERARCHY
          || param == EventSearchParameter.VERBATIM_EVENT_TYPE) {
        continue;
      }

      String name = null;
      if (param == EventSearchParameter.MEASUREMENT_TYPE_ID
          || param == EventSearchParameter.FUNDING_ATTRIBUTION_ID) {
        name =
            CaseFormat.LOWER_UNDERSCORE
                .to(CaseFormat.LOWER_CAMEL, param.name())
                .replace("Id", "ID");
      } else {
        name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, param.name());
      }

      assertTrue(
          "Search parameter " + param + "/" + name + " is not documented",
          documentedParameters.contains(name));
    }
  }
}
