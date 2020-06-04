package org.gbif.metrics.ws.client;

import org.gbif.api.model.metrics.cube.ReadBuilder;
import org.gbif.api.model.metrics.cube.Rollup;
import org.gbif.api.service.metrics.CubeService;

import java.util.List;

import com.google.common.base.Preconditions;
import org.springframework.cloud.openfeign.SpringQueryMap;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Client-side implementation to the generic cube service.
 */
public interface CubeWsClient extends CubeService {


  String OCCURRENCE_COUNT_PATH = "occurrence/count";
  String OCCURRENCE_SCHEMA_PATH = "occurrence/count/schema";

  @Override
  default long get(@SpringQueryMap ReadBuilder addressBuilder) throws IllegalArgumentException {
    Preconditions.checkNotNull(addressBuilder, "The cube address is mandatory");
    LinkedMultiValueMap<String,String> requestMap = new LinkedMultiValueMap<>();
    addressBuilder.build().entrySet().forEach( e ->
      requestMap.add(e.getKey().getKey(), e.getValue())
      );
    return get(requestMap);
  }


  @RequestMapping(
    method = RequestMethod.GET,
    value = OCCURRENCE_COUNT_PATH,
    produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  long get(@SpringQueryMap MultiValueMap<String,String> addressBuilder) throws IllegalArgumentException;

  @RequestMapping(
    method = RequestMethod.GET,
    value = OCCURRENCE_SCHEMA_PATH,
    produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  List<Rollup> getSchema();

}
