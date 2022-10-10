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
package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.geospatial.CoordinateParseUtils;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.geocode.api.model.Location;
import org.gbif.geocode.api.service.GeocodeService;
import org.gbif.occurrence.processor.interpreting.clients.GeocodeWsClient;
import org.gbif.occurrence.processor.interpreting.result.CoordinateResult;
import org.gbif.occurrence.processor.interpreting.util.CountryMaps;
import org.gbif.occurrence.processor.interpreting.util.Wgs84Projection;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.util.*;
import java.util.function.BiFunction;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Lists;

/**
 * Attempts to parse given string latitude and longitude into doubles, and compares the given country (if any) to a reverse
 * lookup of the parsed coordinates. If no country was given and the lookup produced something, that looked up result
 * is returned. If the lookup result and passed in country don't match, a "GeospatialIssue" is noted.
 */
public class CoordinateInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinateInterpreter.class);

  // Coordinate transformations to attempt in order
  private static final Map<List<OccurrenceIssue>, BiFunction<Double, Double, LatLng>> TRANSFORMS = new LinkedHashMap<>();

  // Antarctica: "Territories south of 60° south latitude"
  private static final double ANTARCTICA_LATITUDE = -60;

  private GeocodeService geocodeClient;

  static {
    TRANSFORMS.put(Collections.emptyList(), LatLng::new);
    TRANSFORMS.put(Collections.singletonList(OccurrenceIssue.PRESUMED_NEGATED_LATITUDE), (lat, lng) -> new LatLng(-1 * lat, lng));
    TRANSFORMS.put(Collections.singletonList(OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE), (lat, lng) -> new LatLng(lat, -1 * lng));
    TRANSFORMS.put(Arrays.asList(OccurrenceIssue.PRESUMED_NEGATED_LATITUDE, OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE), (lat, lng) -> new LatLng(-1 * lat, -1 * lng));
    TRANSFORMS.put(Collections.singletonList(OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE), (lat, lng) -> new LatLng(lng, lat));
  }


  /**
   * Should not be instantiated.
   * @param apisWsUrl API webservice base URL
   */
  @Autowired
  public CoordinateInterpreter(String apisWsUrl) {
    this.geocodeClient =
      new ClientBuilder()
        .withUrl(apisWsUrl)
        .withFormEncoder()
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .build(GeocodeWsClient.class);
  }

  /**
   * Create with a custom GeocodeService, e.g. shapefiles.
   */
  public CoordinateInterpreter(GeocodeService geocodeService) {
    this.geocodeClient = geocodeService;
  }

  /**
   * Attempts to convert the given lat and long into Doubles, and the given country string into an ISO country code.
   *
   * @param latitude  decimal latitude as string
   * @param longitude decimal longitude as string
   * @param country   country as interpreted to sanity check coordinate
   *
   * @return the latitude and longitude as doubles, the country as an ISO code, and issues if any
   * known errors were encountered in the interpretation (e.g. lat/lng reversed).
   * Or all fields set to null if latitude or longitude are null
   */
  public OccurrenceParseResult<CoordinateResult> interpretCoordinate(String latitude, String longitude, String datum, final Country country) {
    return verifyLatLon(CoordinateParseUtils.parseLatLng(latitude, longitude), datum, country);
  }

  /**
   * @param latLon a verbatim coordinate string containing both latitude and longitude
   * @param country   country as interpreted to sanity check coordinate
   */
  public OccurrenceParseResult<CoordinateResult> interpretCoordinate(String latLon, String datum, final Country country) {
    return verifyLatLon(CoordinateParseUtils.parseVerbatimCoordinates(latLon), datum, country);
  }

  private OccurrenceParseResult<CoordinateResult> verifyLatLon(final OccurrenceParseResult<LatLng> parsedLatLon, final String datum, final Country country) {
    // use original as default
    Country finalCountry = country;
    final Set<OccurrenceIssue> issues = EnumSet.noneOf(OccurrenceIssue.class);

    issues.addAll(parsedLatLon.getIssues());
    if (!parsedLatLon.isSuccessful()) {
      return OccurrenceParseResult.fail(issues);
    }

    // interpret geodetic datum and reproject if needed
    // the reprojection will keep the original values even if it failed with issues
    OccurrenceParseResult<LatLng> projectedLatLon = Wgs84Projection.reproject(parsedLatLon.getPayload().getLat(), parsedLatLon.getPayload().getLng(), datum);
    issues.addAll(projectedLatLon.getIssues());

    LatLng coord = projectedLatLon.getPayload();

    // Try each possible way of transforming the co-ordinates; the first is the identity transform.
    OccurrenceParseResult<LatLng> interpretedLatLon = null;
    boolean identityTransform = true;
    for (Map.Entry<List<OccurrenceIssue>, BiFunction<Double, Double, LatLng>> geospatialTransform : TRANSFORMS.entrySet()) {
      BiFunction<Double, Double, LatLng> transform = geospatialTransform.getValue();
      List<OccurrenceIssue> transformIssues = geospatialTransform.getKey();

      LatLng tCoord = transform.apply(coord.getLat(), coord.getLng());
      List<Country> latLngCountries = getCountryForLatLng(tCoord);
      Country matchCountry = matchCountry(country, latLngCountries, issues, identityTransform);
      if (country == null || matchCountry != null) {
        // Either we don't have a country, in which case we don't want to try anything other than
        // the initial identity transform, or we have a match from this transform.

        if (country == null && latLngCountries.size() > 0) {
          finalCountry = latLngCountries.get(0);
          issues.add(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);
        } else {
          // Take the returned country, in case we had a confused match.
          finalCountry = matchCountry;
        }

        // Use the changed co-ordinates
        interpretedLatLon = OccurrenceParseResult.fail(tCoord, transformIssues);
        break;
      }

      identityTransform = false;
    }

    if (interpretedLatLon == null) {
      // Transformations failed
      interpretedLatLon = OccurrenceParseResult.fail(coord, OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH);
    }

    issues.addAll(interpretedLatLon.getIssues());

    if (interpretedLatLon.getPayload() == null) {
      // something has gone very wrong
      LOG.warn("Supposed coordinate interpretation success produced no latlng {}", interpretedLatLon);
      return OccurrenceParseResult.fail(issues);
    }

    return OccurrenceParseResult.success(interpretedLatLon.getConfidence(),
                               new CoordinateResult(interpretedLatLon.getPayload(), finalCountry),  issues);
  }

  /**
   * @return true if the given country (or its oft-confused neighbours) is one of the potential countries given
   */
  private static Country matchCountry(Country country, List<Country> potentialCountries, Set<OccurrenceIssue> issues, boolean identityTransform) {
    // If we don't have a supplied country, just return the first
    if (country == null && potentialCountries.size() > 0) {
      return potentialCountries.get(0);
    }

    // First check for the country in the potential countries
    if (potentialCountries.contains(country)) {
      return country;
    }

    // Only use the country equivalences if the coordinates haven't been changed.
    if (identityTransform) {

      // Then check with acceptable equivalent countries — no issue is added.
      Set<Country> equivalentCountries = CountryMaps.equivalent(country);
      if (equivalentCountries != null) {
        for (Country pCountry : potentialCountries) {
          if (equivalentCountries.contains(pCountry)) {
            return pCountry;
          }
        }
      }

      // Then also check with commonly confused neighbours — an issue is added.
      Set<Country> confusedCountries = CountryMaps.confused(country);
      if (confusedCountries != null) {
        for (Country pCountry : potentialCountries) {
          if (confusedCountries.contains(pCountry)) {
            issues.add(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);
            return pCountry;
          }
        }
      }
    }

    return null;
  }

  /**
   * Checks if the country and latitude belongs to Antarctica.
   * Rule: country must be Country.ANTARCTICA or null and
   * latitude must be less than (south of) {@link #ANTARCTICA_LATITUDE}
   * but not less than -90°.
   *
   * @param latitude
   * @param country null allowed
   * @return
   */
  private static boolean isAntarctica(Double latitude, @Nullable Country country){
    if (latitude == null) {
      return false;
    }

    return (country == null || country == Country.ANTARCTICA) && (latitude >= -90 && latitude < ANTARCTICA_LATITUDE);
  }

  /**
   * It's theoretically possible that the webservice could respond with more than one country, though it's not
   * known under what conditions that might happen.
   *
   * It happens when we are within 100m of a border, then both countries are returned.
   */
  private List<Country> getCountryForLatLng(LatLng coord) {
    List<Country> countries = Lists.newArrayList();

    Double latitude = coord.getLat();
    Double longitude = coord.getLng();
    if (latitude == null || longitude == null
            || latitude < -90 || latitude > 90
            || longitude < -180 || longitude > 180) {
      // Don't bother sending the request.
      return Collections.emptyList();
    }

    try {
      Collection<Location> response = geocodeClient.get(coord.getLat(), coord.getLng(), null, null, Collections.singletonList("Political"));

      if (response != null) {
        LOG.debug("Successfully retrieved [{}] locations for coord {}", response.size(), coord);
        for (Location loc : response) {
          if (loc.getIsoCountryCode2Digit() != null) {
            countries.add(Country.fromIsoCode(loc.getIsoCountryCode2Digit()));
          }
        }
        LOG.debug("Countries are {}", countries);
      }
      else if (isAntarctica(coord.getLat(), null)) {
        // If no country is returned from the geocode, add Antarctica if we're sufficiently far south
        countries.add(Country.ANTARCTICA);
      }
    } catch (Exception e) {
      // Log the error
      LOG.error("Failed to lookup coordinate {}, {}", coord, e.getMessage());
      throw new RuntimeException(e);
    }

    return countries;
  }
}
