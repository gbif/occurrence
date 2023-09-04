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
package org.gbif.occurrence.processor.interpreting.result;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.geospatial.LatLng;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.google.common.base.Objects;

/**
 * The immutable result of a Coordinate interpretation.
 */
public class CoordinateResult {

  private final Double latitude;
  private final Double longitude;
  private final Country country;

  public CoordinateResult(LatLng coord, Country country) {
    this.latitude = coord.getLat();
    this.longitude = coord.getLng();
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
    final CoordinateResult other = (CoordinateResult) obj;
    return Objects.equal(this.latitude, other.latitude)
           && Objects.equal(this.longitude, other.longitude)
           && Objects.equal(this.country, other.country);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("country", getCountry())
        .append("latitude", getLatitude())
        .append("longitude", getLongitude()).toString();
  }
}
