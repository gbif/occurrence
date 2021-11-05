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

import org.gbif.api.service.occurrence.OccurrenceCountryIndexService;
import org.gbif.api.vocabulary.Country;

import java.util.Map;
import java.util.function.Function;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * A web service client to support the accession of occurrence dataset indexes.
 */
public interface OccurrenceCountryIndexWsClient extends OccurrenceCountryIndexService {

  Function<Country,String> COUNTRY_TO_ISO2 = Country::getIso2LetterCode;

  @Override
  default Map<Country, Long> publishingCountriesForCountry(Country country) {
    return publishingCountriesForCountry(COUNTRY_TO_ISO2.apply(country));
  }

  @RequestMapping(
    method = RequestMethod.GET,
    value = "occurrence/counts/publishingCountries",
    produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  Map<Country, Long> publishingCountriesForCountry(@RequestParam("country") String country);

  @Override
  default Map<Country, Long> countriesForPublishingCountry(Country publishingCountry){
    return countriesForPublishingCountry(COUNTRY_TO_ISO2.apply(publishingCountry));
  }

  @RequestMapping(
    method = RequestMethod.GET,
    value = "occurrence/counts/countries",
    produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  Map<Country, Long> countriesForPublishingCountry(@RequestParam("publishingCountry") String publishingCountry);

}
