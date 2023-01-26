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

import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class to validate PredicateDownloadRequest against a Json Schema.
 * This class is implemented base on schemas because Jackson doesn't recognise unknown properties when objects
 * are created using the JsonCreator annotation on a constructor.
 */
@Slf4j
public class DownloadRequestsValidator {

  private static final ObjectMapper MAPPER = JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport();

  static {
    MAPPER.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  }


  /** Validates the predicate download request as Json against the schema.*/
  public void validate(String jsonPredicateDownloadRequest) {
    try {
      PredicateDownloadRequest request = MAPPER.readValue(jsonPredicateDownloadRequest, PredicateDownloadRequest.class);
      if (Objects.isNull(request)) {
        throw new IllegalArgumentException("Request contains invalid fields");
      }
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException(exception);
    }
  }
}
