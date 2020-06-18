package org.gbif.occurrence.processor.interpreting.clients;

import org.gbif.api.model.checklistbank.NameUsage;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@RequestMapping(
  method = RequestMethod.GET,
  value = "species",
  produces = MediaType.APPLICATION_JSON_VALUE)
public interface SpeciesWsClient {

  @RequestMapping(
    value = "{key}"
  )
  NameUsage get(@PathVariable("key") String nubKey);

}
