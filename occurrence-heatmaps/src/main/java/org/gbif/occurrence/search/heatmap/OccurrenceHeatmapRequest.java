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
package org.gbif.occurrence.search.heatmap;


import org.gbif.occurrence.search.predicate.OccurrencePredicateSearchRequest;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Request class for issuing heat map search request to the occurrence search service.
 */
public class OccurrenceHeatmapRequest extends OccurrencePredicateSearchRequest {

  public enum Mode {
    GEO_BOUNDS, GEO_CENTROID;
  }

  private String geometry;

  private int zoom;

  private Mode mode = Mode.GEO_BOUNDS;

  private int bucketLimit = 15000;

  /**
   * Default empty constructor, required for serialization.
   */
  public OccurrenceHeatmapRequest() {
    this.limit = 0;
    this.offset = 0;
  }

  /**
   * The region to compute the heatmap on, specified using the rectangle-range syntax or WKT.
   * It defaults to the world. ex: ["-180 -90" TO "180 90"].
   */
  public String getGeometry() {
    return geometry;
  }

  public void setGeometry(String geometry) {
    this.geometry = geometry;
  }

  /**
   * Heatmap zoom/gridLevel level
   * @return
   */
  public int getZoom() {
    return zoom;
  }

  public void setZoom(int zoom) {
    this.zoom = zoom;
  }

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public int getBucketLimit() {
    return bucketLimit;
  }

  public void setBucketLimit(int bucketLimit) {
    this.bucketLimit = bucketLimit;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("geometry", geometry).add("zoom",zoom)
            .add("mode", mode).add("bucketLimit", bucketLimit).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OccurrenceHeatmapRequest)) {
      return false;
    }

    OccurrenceHeatmapRequest that = (OccurrenceHeatmapRequest) obj;
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
