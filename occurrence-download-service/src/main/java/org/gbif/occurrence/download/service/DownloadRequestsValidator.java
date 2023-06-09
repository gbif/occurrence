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

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;


/**
 * Utility class to validate PredicateDownloadRequest against a Json Schema.
 * This class is implemented base on schemas because Jackson doesn't recognise unknown properties when objects
 * are created using the JsonCreator annotation on a constructor.
 */
@Slf4j
public class DownloadRequestsValidator {
  private static final Logger LOG = LoggerFactory.getLogger(DownloadRequestsValidator.class);

  public DownloadRequestsValidator() {
  }


  /** Validates the predicate download request as Json against the schema.*/
  @SneakyThrows
  public void validate(String jsonPredicateDownloadRequest) {
    JSONObject object = new JSONObject(jsonPredicateDownloadRequest);
    if (!object.has("predicate")) {
      LOG.warn("Download request without a predicate: {}", jsonPredicateDownloadRequest);
    }
  }
}
