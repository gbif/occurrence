package org.gbif.search.heatmap.es.occurrence;

import java.util.Optional;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.search.es.BaseEsSearchRequestBuilder;
import org.gbif.search.heatmap.es.EsHeatmapRequestBuilder;
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequest;
import org.gbif.predicate.query.EsFieldMapper;

public class OccurrenceEsHeatmapRequestBuilder
    extends EsHeatmapRequestBuilder<OccurrenceSearchParameter, OccurrenceHeatmapRequest> {

  OccurrenceEsHeatmapRequestBuilder(
      EsFieldMapper<OccurrenceSearchParameter> esFieldMapper,
      BaseEsSearchRequestBuilder<OccurrenceSearchParameter, OccurrenceHeatmapRequest>
          esSearchRequestBuilder) {
    super(esFieldMapper, esSearchRequestBuilder);
  }

  @Override
  protected Optional<OccurrenceSearchParameter> getParam(String name) {
    return OccurrenceSearchParameter.lookup(name);
  }
}
