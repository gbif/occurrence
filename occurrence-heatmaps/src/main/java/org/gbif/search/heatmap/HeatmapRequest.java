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
package org.gbif.search.heatmap;



/**
 * Request class for issuing heat map search request to the search service.
 */
public interface HeatmapRequest {

  enum Mode {
    GEO_BOUNDS, GEO_CENTROID;
  }

  /**
   * The region to compute the heatmap on, specified using the rectangle-range syntax or WKT.
   * It defaults to the world. ex: ["-180 -90" TO "180 90"].
   */
  String getGeometry();

  void setGeometry(String geometry);

  /**
   * Heatmap zoom/gridLevel level
   * @return
   */
  int getZoom();

  void setZoom(int zoom);

  Mode getMode();

  void setMode(Mode mode);

  int getBucketLimit();

  void setBucketLimit(int bucketLimit);

}
