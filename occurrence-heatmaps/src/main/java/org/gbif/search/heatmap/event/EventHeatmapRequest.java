package org.gbif.search.heatmap.event;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.gbif.api.model.event.search.EventPredicateSearchRequest;
import org.gbif.search.heatmap.HeatmapRequest;

/** Request class for issuing heat map search request to the event search service. */
public class EventHeatmapRequest extends EventPredicateSearchRequest implements HeatmapRequest {

  private String geometry;

  private int zoom;

  private Mode mode = Mode.GEO_BOUNDS;

  private int bucketLimit = 15000;

  /** Default empty constructor, required for serialization. */
  public EventHeatmapRequest() {
    this.limit = 0;
    this.offset = 0;
  }

  @Override
  public String getGeometry() {
    return geometry;
  }

  @Override
  public void setGeometry(String geometry) {
    this.geometry = geometry;
  }

  @Override
  public int getZoom() {
    return zoom;
  }

  @Override
  public void setZoom(int zoom) {
    this.zoom = zoom;
  }

  @Override
  public Mode getMode() {
    return mode;
  }

  @Override
  public void setMode(Mode mode) {
    this.mode = mode;
  }

  @Override
  public int getBucketLimit() {
    return bucketLimit;
  }

  @Override
  public void setBucketLimit(int bucketLimit) {
    this.bucketLimit = bucketLimit;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("geometry", geometry)
        .add("zoom", zoom)
        .add("mode", mode)
        .add("bucketLimit", bucketLimit)
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof EventHeatmapRequest)) {
      return false;
    }

    EventHeatmapRequest that = (EventHeatmapRequest) obj;
    return Objects.equal(geometry, that.geometry)
        && Objects.equal(zoom, that.zoom)
        && Objects.equal(mode, that.mode)
        && Objects.equal(bucketLimit, that.bucketLimit);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), geometry, zoom, mode, bucketLimit);
  }
}
