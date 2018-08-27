package org.gbif.occurrence.search.heatmap.solr;

import java.util.List;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * This class contains the response of a occurrence heat map search.
 */
public class SolrOccurrenceHeatmapResponse {

  private Integer columns;
  private Integer rows;
  private Long count;
  private Double minX;
  private Double maxX;
  private Double minY;
  private Double maxY;
  private Double lengthX;
  private Double lengthY;
  private List<List<Integer>> countsInts2D;

  /**
   * Default constructor, required for JSON serialization.
   */
  public SolrOccurrenceHeatmapResponse() {
    //empty
  }

  /**
   * Full constructor.
   */
  public SolrOccurrenceHeatmapResponse(
    Integer columns,
    Integer rows,
    Long count,
    Double minX,
    Double maxX,
    Double minY,
    Double maxY,
    List<List<Integer>> countsInts2D
  ) {
    this.columns = columns;
    this.count = count;
    this.rows = rows;
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    this.countsInts2D = countsInts2D;
    lengthX = (maxX - minX) / columns;
    lengthY = (maxY - minY) / rows;

  }

  /**
   *
   * @return the number of columns
   */
  public Integer getColumns() {
    return columns;
  }

  public void setColumns(Integer columns) {
    this.columns = columns;
  }

  /**
   *
   * @return number of rows
   */
  public Integer getRows() {
    return rows;
  }

  public void setRows(Integer rows) {
    this.rows = rows;
  }

  /**
   *
   * @return the total count of records.
   */
  public Long getCount() {
    return count;
  }

  public void setCount(Long count) {
    this.count = count;
  }

  /**
   *
   * @return minimum X coordinate
   */
  public Double getMinX() {
    return minX;
  }

  public void setMinX(Double minX) {
    this.minX = minX;
  }

  /**
   *
   * @return maximum X coordinate
   */
  public Double getMaxX() {
    return maxX;
  }

  public void setMaxX(Double maxX) {
    this.maxX = maxX;
  }

  /**
   *
   * @return minimum Y coordinate
   */
  public Double getMinY() {
    return minY;
  }

  public void setMinY(Double minY) {
    this.minY = minY;
  }

  /**
   *
   * @return maximum Y coordinate
   */
  public Double getMaxY() {
    return maxY;
  }

  public void setMaxY(Double maxY) {
    this.maxY = maxY;
  }

  /**
   *
   * @return length of X: maxX - minX
   */
  public Double getLengthX() {
    if(lengthX == null){
      lengthX = (maxX - minX) / columns;
    }
    return lengthX;
  }

  /**
   *
   * @return length of Y: maxY - minY
   */
  public Double getLengthY() {
    if(lengthY == null){
      lengthY = (maxY - minY) / columns;
    }
    return lengthY;
  }

  /**
   *
   * @return record count on each cell
   */
  public List<List<Integer>> getCountsInts2D() {
    return countsInts2D;
  }

  public void setCountsInts2D(List<List<Integer>> countsInts2D) {
    this.countsInts2D = countsInts2D;
  }

  /**
   *
   * @param column number
   * @return the minimum longitude for the column number
   */
  public Double getMinLng(int column) {
    return minX + (lengthX * column);
  }

  /**
   *
   * @param row number
   * @return the minimum latitude for the row number
   */
  public Double getMinLat(int row) {
    return maxY - (lengthY * row) - lengthY;
  }

  /**
   *
   * @param column number
   * @return the maximum longitude for the column number
   */
  public Double getMaxLng(int column) {
    return minX + (lengthX * column) + lengthX;
  }

  /**
   *
   * @param row number
   * @return the maximum latitude for the row number
   */
  public Double getMaxLat(int row) {
    return maxY - (lengthY * row);
  }


  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("columns", columns).add("rows", rows).add("count", count).add("minX", minX)
      .add("maxX",maxX).add("minY",minY).add("maxY",maxY).add("lengthX",lengthX).add("lenghtY",lengthY)
      .add("countsInts2D",countsInts2D).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SolrOccurrenceHeatmapResponse)) {
      return false;
    }

    SolrOccurrenceHeatmapResponse that = (SolrOccurrenceHeatmapResponse) obj;
    return Objects.equal(columns, that.columns)
           && Objects.equal(rows, that.rows)
           && Objects.equal(count, that.count)
           && Objects.equal(minX, that.minX)
           && Objects.equal(maxX, that.maxX)
           && Objects.equal(minY, that.minY)
           && Objects.equal(maxY, that.maxY)
           && Objects.equal(lengthX, that.lengthX)
           && Objects.equal(lengthY, that.lengthY)
           && Objects.equal(countsInts2D, that.countsInts2D);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(columns,rows,count,minX,maxX,minY,maxY,lengthX,lengthY,countsInts2D);
  }

}
