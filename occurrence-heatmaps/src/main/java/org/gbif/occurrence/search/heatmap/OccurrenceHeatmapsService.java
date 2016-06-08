package org.gbif.occurrence.search.heatmap;

import org.gbif.common.search.exception.SearchException;
import org.gbif.occurrence.search.OccurrenceSearchRequestBuilder;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;

import java.io.IOException;
import javax.annotation.Nullable;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.lucene.spatial.prefix.HeatmapFacetCounter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.params.FacetParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.common.search.util.SolrConstants.SOLR_REQUEST_HANDLER;

public class OccurrenceHeatmapsService {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceHeatmapsService.class);

  private final SolrClient solrClient;

  private final OccurrenceSearchRequestBuilder occurrenceSearchHeatmapRequestBuilder;

  @Inject
  public OccurrenceHeatmapsService(SolrClient solrClient, @Named(SOLR_REQUEST_HANDLER) String requestHandler){
    this.solrClient = solrClient;
    occurrenceSearchHeatmapRequestBuilder = new OccurrenceSearchRequestBuilder(requestHandler, null,1,1,true);
  }

  public OccurrenceHeatmapResponse searchHeatMap(@Nullable OccurrenceHeatmapRequest request) {
    try {
      SolrQuery solrQuery = occurrenceSearchHeatmapRequestBuilder.build(request);
      solrQuery.setRows(0);
      solrQuery.setStart(0);
      solrQuery.setFacet(true);
      solrQuery.add(FacetParams.FACET_HEATMAP, OccurrenceSolrField.COORDINATE.getFieldName());
      solrQuery.add(FacetParams.FACET_HEATMAP_LEVEL, Integer.toString(request.getZoom()));
      solrQuery.add(FacetParams.FACET_HEATMAP_MAX_CELLS, Integer.toString(HeatmapFacetCounter.MAX_ROWS_OR_COLUMNS));
      if(request.getGeometry() != null) {
        solrQuery.add(FacetParams.FACET_HEATMAP_GEOM, request.getGeometry());
      }
      LOG.debug("Solr heatmap query {}", solrQuery);
      return OccurrenceHeatmapResponseBuilder.build(solrClient.query(solrQuery), OccurrenceSolrField.COORDINATE.getFieldName());
    } catch (SolrServerException | IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }
  }
}
