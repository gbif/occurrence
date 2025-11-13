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
package org.gbif.event.ws.resource;

import static org.gbif.event.ws.docs.OpenAPIUtils.*;

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.gbif.api.annotation.NullToNotFound;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.event.Event;
import org.gbif.api.model.event.Lineage;
import org.gbif.api.model.event.search.EventPredicateSearchRequest;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.event.search.EventSearchRequest;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.event.search.es.EventSearchEs;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@OpenAPIDefinition(
    info =
        @Info(
            title = "Event API",
            version = "v1",
            description =
                "This API works against the GBIF Event Store, which handles event records and makes them available "
                    + "through the web service and download files.\n",
            termsOfService = "https://www.gbif.org/terms"),
    servers = {@Server(url = "https://api.gbif-test.org/v1/", description = "User testing")},
    tags = {
      // This is an additional tag to allow the statistics methods implemented in the
      // registry to belong in their own section.
      // They are annotated with @Tag(name = "Event download statistics").
      @Tag(
          name = "Event download statistics",
          description = "This API provides statistics about event downloads.",
          extensions =
              @Extension(
                  name = "Order",
                  properties = @ExtensionProperty(name = "Order", value = "0500"))),
      // And this for the GADM methods from geocode-ws
      @Tag(
        name = "GADM regions",
        description = "The [GADM Global Administrative Area Database](https://gadm.org) " +
          "is a high-resolution database of administrative areas of countries.\n\n" +
          "Within GBIF, it is used to index event data by administrative region.\n\n" +
          "This API provides services to search and browse regions and sub-regions " +
          "down to the third level sub-region.",
        extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "0800"))
      )
    })
@Tag(
    name = "Events",
    description = "This API provides services related to the retrieval of single event records.",
    extensions =
        @io.swagger.v3.oas.annotations.extensions.Extension(
            name = "Order",
            properties = @ExtensionProperty(name = "Order", value = "0100")))
@RestController
@RequestMapping(
    value = "event",
    produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"})
public class EventResource {

  private final EventSearchEs eventSearchEs;

  @Autowired
  public EventResource(EventSearchEs eventSearchEs) {
    this.eventSearchEs = eventSearchEs;
  }

  @Operation(
      operationId = "getEventById",
      summary = "Event by id",
      description =
          "Retrieve details for a single, interpreted event.\n\n"
              + "The returned event includes additional fields, not shown in the response below.  They are verbatim "
              + "fields which are not interpreted by GBIF's system. The names are the short Darwin Core "
              + "Term names.",
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0100")))
  @IdPathParameter
  @ApiResponse(responseCode = "200", description = "Event found")
  @EventErrorResponses
  @NullToNotFound
  @GetMapping("{id}")
  public Event get(@PathVariable("id") String id) {
    return eventSearchEs.get(id);
  }

  @Operation(
      operationId = "getEventByDatasetKeyAndEventId",
      summary = "Event by dataset key and event id",
      description =
          "Retrieve details for a single, interpreted event.\n\n"
              + "The returned event includes additional fields, not shown in the response below.  They are verbatim "
              + "fields which are not interpreted by GBIF's system. The names are the short Darwin Core "
              + "Term names.",
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0200")))
  @DatasetKeyPathParameter
  @EventIdPathParameter
  @ApiResponse(responseCode = "200", description = "Event found")
  @EventErrorResponses
  @NullToNotFound
  @GetMapping("{datasetKey}/{eventId}")
  public Event get(
      @PathVariable("datasetKey") String datasetKey, @PathVariable("eventId") String eventId) {
    return eventSearchEs.get(datasetKey, eventId);
  }

  @Operation(
      operationId = "getParentEventById",
      summary = "Parent event by id",
      description =
          "Retrieve details for a single, interpreted event.\n\n"
              + "The returned event is the parent of the event whose ID has been specified and includes additional fields, "
              + "not shown in the response below.  They are verbatim fields which are not interpreted by GBIF's system. "
              + "The names are the short Darwin Core Term names.",
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0300")))
  @IdPathParameter
  @ApiResponse(responseCode = "200", description = "Event found")
  @EventErrorResponses
  @NullToNotFound
  @GetMapping("{id}/parent")
  public Event getParentEvent(@PathVariable("id") String id) {
    return eventSearchEs.getParentEvent(id).orElse(null);
  }

  @Operation(
      operationId = "getParentEventByDatasetKeyAndEventId",
      summary = "Parent event by dataset key and event id",
      description =
          "Retrieve details for a single, interpreted event.\n\n"
              + "The returned event is the parent of the event whose dataset key and event ID has been specified and includes "
              + " additional fields, not shown in the response below.  They are verbatim fields which are not interpreted by GBIF's system. "
              + "The names are the short Darwin Core Term names.",
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0400")))
  @DatasetKeyPathParameter
  @EventIdPathParameter
  @ApiResponse(responseCode = "200", description = "Event found")
  @EventErrorResponses
  @NullToNotFound
  @GetMapping("{datasetKey}/{eventId}/parent")
  public Event getParentEvent(
      @PathVariable("datasetKey") String datasetKey, @PathVariable("eventId") String eventId) {
    return eventSearchEs.getParentEvent(datasetKey, eventId).orElse(null);
  }

  @Operation(
      operationId = "getLineageById",
      summary = "Lineage of the event by id",
      description = "Retrieves all the parent events of an event.",
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0500")))
  @IdPathParameter
  @ApiResponse(responseCode = "200", description = "Lineage found")
  @EventErrorResponses
  @NullToNotFound
  @GetMapping("{id}/lineage")
  public List<Lineage> getLineage(@PathVariable("id") String id) {
    return eventSearchEs.lineage(id);
  }

  @Operation(
      operationId = "getLineageByDatasetKeyAndEventId",
      summary = "Lineage of the event by dataset key and event id",
      description = "Retrieves all the parent events of an event.",
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0600")))
  @DatasetKeyPathParameter
  @EventIdPathParameter
  @ApiResponse(responseCode = "200", description = "Lineage found")
  @EventErrorResponses
  @NullToNotFound
  @GetMapping("{datasetKey}/{eventId}/lineage")
  public List<Lineage> getLineage(
      @PathVariable("datasetKey") String datasetKey, @PathVariable("eventId") String eventId) {
    return eventSearchEs.lineage(datasetKey, eventId);
  }

  @Operation(
      operationId = "getOccurrencesById",
      summary = "Occurrences of an event by id",
      description = "Retrieves all the interpreted occurrences an event.",
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0700")))
  @IdPathParameter
  @ApiResponse(responseCode = "200", description = "Occurrences found")
  @EventErrorResponses
  @NullToNotFound
  @GetMapping("{id}/occurrences")
  public PagingResponse<Occurrence> getOccurrences(
      @PathVariable("id") String id, @NotNull @Valid PagingRequest pagingRequest) {
    return eventSearchEs.occurrences(id, pagingRequest);
  }

  @Operation(
      operationId = "getOccurrencesByDatasetKeyAndEventId",
      summary = "Occurrences of an event by dataset key and event id",
      description = "Retrieves all the interpreted occurrences of an event.",
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0800")))
  @DatasetKeyPathParameter
  @EventIdPathParameter
  @ApiResponse(responseCode = "200", description = "Event found")
  @EventErrorResponses
  @NullToNotFound
  @GetMapping("{datasetKey}/{eventId}/occurrences")
  public PagingResponse<Occurrence> getOccurrences(
      @PathVariable("datasetKey") String datasetKey,
      @PathVariable("eventId") String eventId,
      @NotNull @Valid PagingRequest pagingRequest) {
    return eventSearchEs.occurrences(datasetKey, eventId, pagingRequest);
  }

  @Operation(
      operationId = "getSubeventsById",
      summary = "Subevents of an event by id",
      description = "Retrieves all the children events of an event.",
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0900")))
  @IdPathParameter
  @ApiResponse(responseCode = "200", description = "Subevents found")
  @EventErrorResponses
  @NullToNotFound
  @GetMapping("{id}/subEvents")
  public PagingResponse<Event> subEvents(
      @PathVariable("id") String id, @NotNull @Valid PagingRequest pagingRequest) {
    return eventSearchEs.subEvents(id, pagingRequest);
  }

  @Operation(
      operationId = "getSubeventsByDatasetKeyAndEventId",
      summary = "Subevents of an event by dataset key and event id",
      description = "Retrieves all the children events of an event.",
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "1000")))
  @DatasetKeyPathParameter
  @EventIdPathParameter
  @ApiResponse(responseCode = "200", description = "Subevents found")
  @EventErrorResponses
  @NullToNotFound
  @GetMapping("{datasetKey}/{eventId}/subEvents")
  public PagingResponse<Event> subEvents(
      @PathVariable("datasetKey") String datasetKey,
      @PathVariable("eventId") String eventId,
      @NotNull @Valid PagingRequest pagingRequest) {
    return eventSearchEs.subEvents(datasetKey, eventId, pagingRequest);
  }

  @Operation(
    operationId = "searchEvent",
    summary = "Event search",
    description = "Full search across all events.",
    extensions =
    @Extension(
      name = "Order",
      properties = @ExtensionProperty(name = "Order", value = "0000")))
  @EventSearchParameters
  @ApiResponses(
    value = {
      @ApiResponse(responseCode = "200", description = "Event search is valid"),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid query, e.g. invalid vocabulary values",
        content = @Content)
    })
  @NullToNotFound
  @GetMapping("search")
  public SearchResponse<Event, EventSearchParameter> search(
      @NotNull @Valid EventSearchRequest request) {
    return eventSearchEs.search(request);
  }

  @Hidden
  @PostMapping("search/predicate/toesquery")
  public String predicateToEsQuery(
      @NotNull @Valid @RequestBody EventPredicateSearchRequest request) {
    return eventSearchEs
        .getEsSearchRequestBuilder()
        .buildQuery(request)
        .map(AbstractQueryBuilder::toString)
        .orElseThrow(() -> new IllegalArgumentException("Request can't be translated"));
  }

  @Hidden
  @GetMapping("search/rest/toesquery")
  public String restToEsQuery(@NotNull @Valid @ParameterObject EventSearchRequest request) {
    return eventSearchEs
        .getEsSearchRequestBuilder()
        .buildSearchRequest(request, "test")
        .source()
        .toString();
  }
}
