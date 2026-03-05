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
package org.gbif.metrics.ws.client;

import org.gbif.api.model.metrics.cube.Dimension;
import org.gbif.api.model.metrics.cube.ReadBuilder;
import org.gbif.api.model.metrics.cube.Rollup;
import org.gbif.api.service.metrics.CubeService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.cloud.openfeign.SpringQueryMap;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;

import com.google.common.base.Preconditions;

/**
 * Client for occurrence-search-ws count and schema endpoints.
 */
public interface CubeWsClient extends CubeService {

  String OCCURRENCE_COUNT_PATH = "occurrence/count";
  String OCCURRENCE_SCHEMA_PATH = "occurrence/count/schema";

  @Override
  default long get(ReadBuilder addressBuilder) throws IllegalArgumentException {
    Preconditions.checkNotNull(addressBuilder, "The cube address is mandatory");
    Map<String, String> params = new HashMap<>();
    for (Entry<Dimension<?>, String> d : addressBuilder.build().entrySet()) {
      params.put(d.getKey().getKey(), d.getValue());
    }
    return count(params);
  }

  @GetMapping(value = OCCURRENCE_COUNT_PATH, produces = MediaType.APPLICATION_JSON_VALUE)
  Long count(@SpringQueryMap Map<String, String> params);

  @GetMapping(value = OCCURRENCE_SCHEMA_PATH, produces = MediaType.APPLICATION_JSON_VALUE)
  @Override
  List<Rollup> getSchema();

}
