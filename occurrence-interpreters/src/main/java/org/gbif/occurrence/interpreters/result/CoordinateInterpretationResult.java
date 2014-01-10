package org.gbif.occurrence.interpreters.result;

import com.google.common.base.Objects;

/**
 * The immutable result of a Coordinate interpretation.
 */
public class CoordinateInterpretationResult {

  private final Double latitude;
  private final Double longitude;
  private final String countryCode;
  private final Integer geoSpatialIssue;

  public CoordinateInterpretationResult() {
    this.latitude = null;
    this.longitude = null;
    this.countryCode = null;
    this.geoSpatialIssue = null;
  }

  public CoordinateInterpretationResult(Double latitude, Double longitude, String countryCode,
    Integer geoSpatialIssue) {
    this.latitude = latitude;
    this.longitude = longitude;
    this.countryCode = countryCode;
    this.geoSpatialIssue = geoSpatialIssue;
  }

  public Double getLatitude() {
    return latitude;
  }

  public Double getLongitude() {
    return longitude;
  }

  public String getCountryCode() {
    return countryCode;
  }

  public Integer getGeoSpatialIssue() {
    return geoSpatialIssue;
  }

  public boolean isEmpty() {
    return latitude == null && longitude == null && countryCode == null && geoSpatialIssue == null;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(latitude, longitude, countryCode, geoSpatialIssue);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final CoordinateInterpretationResult other = (CoordinateInterpretationResult) obj;
    return Objects.equal(this.latitude, other.latitude) && Objects.equal(this.longitude, other.longitude) && Objects
      .equal(this.countryCode, other.countryCode) && Objects.equal(this.geoSpatialIssue, other.geoSpatialIssue);
  }
}
