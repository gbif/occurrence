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
package org.gbif.occurrence.search.heatmap.es;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/** ElasticSearch GeoHashGrid response object. */
public class EsOccurrenceHeatmapResponse {

  /** Latitude and longitude coordinates. */
  public static class Coordinate {
    private Double lat;
    private Double lon;

    /** @return latitude */
    @JsonProperty("lat")
    public Double getLat() {
      return lat;
    }

    public void setLat(Double lat) {
      this.lat = lat;
    }

    /** @return longitude */
    @JsonProperty("lon")
    public Double getLon() {
      return lon;
    }

    public void setLon(Double lon) {
      this.lon = lon;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Coordinate that = (Coordinate) o;
      return Objects.equals(lat, that.lat) && Objects.equals(lon, that.lon);
    }

    @Override
    public int hashCode() {
      return Objects.hash(lat, lon);
    }
  }

  /** Cell bounds represented using top-left and bottom-right coordinates. */
  public static class Bounds {

    private Coordinate topLeft;
    private Coordinate bottomRight;

    /** @return upper left bound */
    @JsonProperty("top_left")
    public Coordinate getTopLeft() {
      return topLeft;
    }

    public void setTopLeft(Coordinate topLeft) {
      this.topLeft = topLeft;
    }

    /** @return bottom right coordinate */
    @JsonProperty("bottom_right")
    public Coordinate getBottomRight() {
      return bottomRight;
    }

    public void setBottomRight(Coordinate bottomRight) {
      this.bottomRight = bottomRight;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Bounds bounds = (Bounds) o;
      return Objects.equals(topLeft, bounds.topLeft)
          && Objects.equals(bottomRight, bounds.bottomRight);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topLeft, bottomRight);
    }
  }

  /** Wrapper class around a set of coordinate bounds. */
  public static class Cell {

    private Bounds bounds;

    /** @return cell top-left and bottom-right bounds. */
    @JsonProperty("bounds")
    public Bounds getBounds() {
      return bounds;
    }

    public void setBounds(Bounds bounds) {
      this.bounds = bounds;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Cell cell = (Cell) o;
      return Objects.equals(bounds, cell.bounds);
    }

    @Override
    public int hashCode() {
      return Objects.hash(bounds);
    }
  }

  /** Bucket/aggregation response. */
  public static class GeoBoundsGridBucket {
    private String key;
    private Long docCount;
    private Cell cell;

    /** @return GeoHash key */
    @JsonProperty("key")
    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    /** @return number of documents in this cell */
    @JsonProperty("doc_count")
    public Long getDocCount() {
      return docCount;
    }

    public void setDocCount(Long docCount) {
      this.docCount = docCount;
    }

    /** @return cell bounds */
    @JsonProperty("cell")
    public Cell getCell() {
      return cell;
    }

    public void setCell(Cell cell) {
      this.cell = cell;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      GeoBoundsGridBucket that = (GeoBoundsGridBucket) o;
      return Objects.equals(key, that.key)
          && Objects.equals(docCount, that.docCount)
          && Objects.equals(cell, that.cell);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, docCount, cell);
    }
  }


  /** Bucket/aggregation response. */
  public static class GeoCentroidGridBucket {
    private String key;
    private Long docCount;
    private Coordinate centroid;

    /** @return GeoHash key */
    @JsonProperty("key")
    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    /** @return number of documents in this cell */
    @JsonProperty("doc_count")
    public Long getDocCount() {
      return docCount;
    }

    public void setDocCount(Long docCount) {
      this.docCount = docCount;
    }

    /** @return cell centroid */
    @JsonProperty("centroid")
    public Coordinate getCentroid() {
      return centroid;
    }

    public void setCentroid(Coordinate centroid) {
      this.centroid = centroid;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      GeoCentroidGridBucket that = (GeoCentroidGridBucket) o;
      return Objects.equals(key, that.key)
        && Objects.equals(docCount, that.docCount)
        && Objects.equals(centroid, that.centroid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, docCount, centroid);
    }
  }



  public static class GeoBoundsResponse {

    private List<GeoBoundsGridBucket> buckets;
    /**
     * @return a list of geo grid buckets
     */
    @JsonProperty("buckets")
    public List<GeoBoundsGridBucket> getBuckets() {
      return buckets;
    }

    public void setBuckets(List<GeoBoundsGridBucket> buckets) {
      this.buckets = buckets;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      GeoBoundsResponse that = (GeoBoundsResponse) o;
      return Objects.equals(buckets, that.buckets);
    }

    @Override
    public int hashCode() {
      return Objects.hash(buckets);
    }
  }

  public static class GeoCentroidResponse {

    private List<GeoCentroidGridBucket> buckets;
    /**
     * @return a list of geo grid buckets
     */
    @JsonProperty("buckets")
    public List<GeoCentroidGridBucket> getBuckets() {
      return buckets;
    }

    public void setBuckets(List<GeoCentroidGridBucket> buckets) {
      this.buckets = buckets;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      GeoCentroidResponse that = (GeoCentroidResponse) o;
      return Objects.equals(buckets, that.buckets);
    }

    @Override
    public int hashCode() {
      return Objects.hash(buckets);
    }
  }
}
