package org.gbif.occurrence.search.heatmap.es;

import java.util.List;
import java.util.Objects;

import org.codehaus.jackson.annotate.JsonProperty;

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
  public static class GeoGridBucket {
    private String key;
    private Integer docCount;
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
    public Integer getDocCount() {
      return docCount;
    }

    public void setDocCount(Integer docCount) {
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
      GeoGridBucket that = (GeoGridBucket) o;
      return Objects.equals(key, that.key)
          && Objects.equals(docCount, that.docCount)
          && Objects.equals(cell, that.cell);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, docCount, cell);
    }
  }

  private List<GeoGridBucket> buckets;

  /** @return a list of geo grid buckets */
  @JsonProperty("buckets")
  public List<GeoGridBucket> getBuckets() {
    return buckets;
  }

  public void setBuckets(List<GeoGridBucket> buckets) {
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
    EsOccurrenceHeatmapResponse that = (EsOccurrenceHeatmapResponse) o;
    return Objects.equals(buckets, that.buckets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(buckets);
  }
}
