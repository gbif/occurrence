package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.geospatial.CoordinateParseUtils;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.geocode.api.model.Location;
import org.gbif.occurrence.processor.interpreting.result.CoordinateResult;
import org.gbif.occurrence.processor.interpreting.util.CountryMaps;
import org.gbif.occurrence.processor.interpreting.util.RetryingWebserviceClient;
import org.gbif.occurrence.processor.interpreting.util.Wgs84Projection;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.core.MultivaluedMap;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Attempts to parse given string latitude and longitude into doubles, and compares the given country (if any) to a reverse
 * lookup of the parsed coordinates. If no country was given and the lookup produced something, that looked up result
 * is returned. If the lookup result and passed in country don't match, a "GeospatialIssue" is noted.
 */
public class CoordinateInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinateInterpreter.class);

  // if the WS is not responding, we drop into a retry count
  private static final int NUM_RETRIES = 15;
  private static final int RETRY_PERIOD_MSEC = 2000;

  // Coordinate transformations to attempt in case of a mismatch
  private static final Map<List<OccurrenceIssue>, Lambda> TRANSFORMS = new HashMap<>();
  interface Lambda { LatLng apply(Double lat, Double lng); } // Revert the commit introducing this line once we are on Java 8.

  // Antarctica: "Territories south of 60° south latitude"
  private static final double ANTARCTICA_LATITUDE = -60;

  static {
    // These can use neater Java 8 lambda expressions once we've upgraded.
    TRANSFORMS.put(Collections.<OccurrenceIssue>emptyList(), new Lambda() { @Override public LatLng apply(Double lat, Double lng) { return new LatLng(lat, lng); }});
    TRANSFORMS.put(Arrays.asList(OccurrenceIssue.PRESUMED_NEGATED_LATITUDE), new Lambda() { @Override public LatLng apply(Double lat, Double lng) { return new LatLng(-1 * lat, lng); }});
    TRANSFORMS.put(Arrays.asList(OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE), new Lambda() { @Override public LatLng apply(Double lat, Double lng) { return new LatLng(lat, -1 * lng); }});
    TRANSFORMS.put(Arrays.asList(OccurrenceIssue.PRESUMED_NEGATED_LATITUDE, OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE), new Lambda() { @Override public LatLng apply(Double lat, Double lng) { return new LatLng(-1 * lat, -1 * lng); }});
    TRANSFORMS.put(Arrays.asList(OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE), new Lambda() { @Override public LatLng apply(Double lat, Double lng) { return new LatLng(lng, lat); }});
  }

  // The repetitive nature of our data encourages use of a light cache to reduce WS load
  private LoadingCache<WebResource, Location[]> CACHE =
    CacheBuilder.newBuilder().maximumSize(10000).expireAfterAccess(10, TimeUnit.MINUTES)
      .build(RetryingWebserviceClient.newInstance(Location[].class, NUM_RETRIES, RETRY_PERIOD_MSEC));

  private final WebResource GEOCODE_WS;

  /**
   * Should not be instantiated.
   * @param apiWs API webservice base URL
   */
  @Inject
  public CoordinateInterpreter(WebResource apiWs) {
    GEOCODE_WS = apiWs.path("geocode/reverse");
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
    for (Map.Entry<List<OccurrenceIssue>, Lambda> geospatialTransform : TRANSFORMS.entrySet()) {
      Lambda transform = geospatialTransform.getValue();
      List<OccurrenceIssue> transformIssues = geospatialTransform.getKey();

      LatLng tCoord = transform.apply(coord.getLat(), coord.getLng());
      List<Country> latLngCountries = getCountryForLatLng(tCoord);
      Country matchCountry = matchCountry(country, latLngCountries, issues);
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
    }

    if (interpretedLatLon == null) {
      // Transformations failed
      interpretedLatLon = OccurrenceParseResult.fail(coord, OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH);
    }

    issues.addAll(interpretedLatLon.getIssues());

    if (interpretedLatLon.getPayload() == null) {
      // something has gone very wrong
      LOG.warn("Supposed coordinate interpretation success produced no latlng", interpretedLatLon);
      return OccurrenceParseResult.fail(issues);
    }

    return OccurrenceParseResult.success(interpretedLatLon.getConfidence(),
                               new CoordinateResult(interpretedLatLon.getPayload(), finalCountry),  issues);
  }

  /**
   * @return true if the given country (or its oft-confused neighbours) is one of the potential countries given
   */
  private static Country matchCountry(Country country, List<Country> potentialCountries, Set<OccurrenceIssue> issues) {
    // If we don't have a supplied country, just return the first
    if (country == null && potentialCountries.size() > 0) {
      return potentialCountries.get(0);
    }

    // First check for the country in the potential countries
    if (potentialCountries.contains(country)) {
      return country;
    }

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

    MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
    queryParams.add("lat", coord.getLat().toString());
    queryParams.add("lng", coord.getLng().toString());

    LOG.debug("Attempt to lookup coord {}", coord);
    WebResource res = null;
    try {
      res = GEOCODE_WS.queryParams(queryParams);
      Location[] lookups = CACHE.get(res);
      if (lookups != null && lookups.length > 0) {
        LOG.debug("Successfully retrieved [{}] locations for coord {}", lookups.length, coord);
        for (Location loc : lookups) {
          if (loc.getIsoCountryCode2Digit() != null) {
            countries.add(Country.fromIsoCode(loc.getIsoCountryCode2Digit()));
          }
        }
        LOG.debug("Countries are {}", countries);
      }
      else if (lookups.length == 0 && isAntarctica(coord.getLat(), null)) {
        // If no country is returned from the geocode, add Antarctica if we're sufficiently far south
        countries.add(Country.ANTARCTICA);
      }
    } catch (Exception e) {
      // Log the error
      LOG.error("Failed WS call with: {}", res.getURI());
      throw new RuntimeException(e);
    }

    return countries;
  }
}
