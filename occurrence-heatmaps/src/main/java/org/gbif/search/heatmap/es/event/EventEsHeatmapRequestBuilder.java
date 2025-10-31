package org.gbif.search.heatmap.es.event;

import java.util.Optional;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.occurrence.search.es.BaseEsSearchRequestBuilder;
import org.gbif.predicate.query.EsFieldMapper;
import org.gbif.search.heatmap.es.EsHeatmapRequestBuilder;
import org.gbif.search.heatmap.event.EventHeatmapRequest;

public class EventEsHeatmapRequestBuilder
    extends EsHeatmapRequestBuilder<EventSearchParameter, EventHeatmapRequest> {

  EventEsHeatmapRequestBuilder(
      EsFieldMapper<EventSearchParameter> esFieldMapper,
      BaseEsSearchRequestBuilder<EventSearchParameter, EventHeatmapRequest>
          esSearchRequestBuilder) {
    super(esFieldMapper, esSearchRequestBuilder);
  }

  @Override
  protected Optional<EventSearchParameter> getParam(String name) {
    return EventSearchParameter.lookupEventParam(name);
  }
}
