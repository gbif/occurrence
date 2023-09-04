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
package org.gbif.occurrence.processor.interpreting.clients;

import org.gbif.geocode.api.model.Location;
import org.gbif.geocode.api.service.GeocodeService;

import java.util.List;

import javax.annotation.Nullable;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@RequestMapping(
  method = RequestMethod.GET,
  value = "geocode",
  produces = MediaType.APPLICATION_JSON_VALUE
)
public interface GeocodeWsClient extends GeocodeService {

  @RequestMapping(
    method = RequestMethod.GET
  )
  @ResponseBody
  @Override
  List<Location> get(@RequestParam("lat") Double lat, @RequestParam("lng") Double lng,
                           @Nullable @RequestParam(value = "uncertaintyDegrees", required = false) Double uncertaintyDegrees,
                           @Nullable @RequestParam(value = "uncertaintyMeters", required = false) Double uncertaintyMeters);

  @RequestMapping(
    method = RequestMethod.GET
  )
  @ResponseBody
  @Override
  List<Location> get(@RequestParam("lat") Double lat, @RequestParam("lng") Double lng,
                                  @Nullable @RequestParam(value = "uncertaintyDegrees", required = false) Double uncertaintyDegrees,
                                  @Nullable @RequestParam(value = "uncertaintyMeters", required = false) Double uncertaintyMeters,
                                  @Nullable @RequestParam(value = "layer", required = false) List<String> layers);

  @RequestMapping(
    method = RequestMethod.GET,
    value = "bitmap"
  )
  @ResponseBody
  @Override
  byte[] bitmap();


}
