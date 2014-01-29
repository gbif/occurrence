package org.gbif.occurrence.processor.interpreting.result;

import org.gbif.api.vocabulary.Country;

import com.google.common.base.Objects;

/**
 * The immutable result of a Coordinate interpretation.
 */
public class CoordinateCountry {

  private final Double latitude;
  private final Double longitude;
  private final Country country;

  public CoordinateCountry(Double latitude, Double longitude, Country country) {
    this.latitude = latitude;
    this.longitude = longitude;
    this.country = country;
  }

  public Double getLatitude() {
    return latitude;
  }

  public Double getLongitude() {
    return longitude;
  }

  public Country getCountry() {
    return country;
  }

  public boolean isEmpty() {
    return latitude == null && longitude == null && country == null;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(latitude, longitude, country);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final CoordinateCountry other = (CoordinateCountry) obj;
    return Objects.equal(this.latitude, other.latitude)
           && Objects.equal(this.longitude, other.longitude)
           && Objects.equal(this.country, other.country);
  }
}
