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
package org.gbif.metrics.ws.service;

import java.util.Map;

import org.gbif.api.model.predicate.Predicate;

/**
 * Occurrence count and inventory metrics.
 * Implementations may cache results.
 */
public interface OccurrenceMetricsService {

  long count(Predicate predicate);

  Map<String, Long> getBasisOfRecordCounts();

  Map<String, Long> getCountries(String publishingCountry);

  Map<String, Long> getDatasets(String country, Integer nubKey, String taxonKey, String checklistKey);

  Map<String, Long> getKingdomCounts(String checklistKey);

  Map<String, Long> getPublishingCountries(String country);

  Map<String, Long> getYearCounts(String year);
}
