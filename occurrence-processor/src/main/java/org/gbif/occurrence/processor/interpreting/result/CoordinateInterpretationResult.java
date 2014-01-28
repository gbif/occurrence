package org.gbif.occurrence.processor.interpreting.result;

import org.gbif.api.vocabulary.Country;

import com.google.common.base.Objects;

/**
 * The immutable result of a Coordinate interpretation.
 */
public class CoordinateInterpretationResult extends InterpretationResult<Country> {

  private final Double latitude;
  private final Double longitude;

  public CoordinateInterpretationResult() {
    super(null);
    this.latitude = null;
    this.longitude = null;
  }

  public CoordinateInterpretationResult(Double latitude, Double longitude, Country country) {
    super(country);
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public Double getLatitude() {
    return latitude;
  }

  public Double getLongitude() {
    return longitude;
  }

  public Country getCountry() {
    return getPayload();
  }

  public boolean isEmpty() {
    return latitude == null && longitude == null && getPayload() == null;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(latitude, longitude);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    if (!super.equals(obj)) {
      return false;
    }
    final CoordinateInterpretationResult other = (CoordinateInterpretationResult) obj;
    return Objects.equal(this.latitude, other.latitude)
           && Objects.equal(this.longitude, other.longitude);
  }
}
