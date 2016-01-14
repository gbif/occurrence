package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.geospatial.CoordinateParseUtils;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.geocode.api.model.Location;
import org.gbif.occurrence.processor.interpreting.result.CoordinateResult;
import org.gbif.occurrence.processor.interpreting.util.RetryingWebserviceClient;
import org.gbif.occurrence.processor.interpreting.util.Wgs84Projection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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

  private static final String CONFUSED_COUNTRY_FILE = "confused-country-pairs.txt";

  // Coordinate transformations to attempt in case of a mismatch
  private static final Map<List<OccurrenceIssue>, Lambda> TRANSFORMS = new HashMap<>();
  interface Lambda { LatLng apply(Double lat, Double lng); } // Revert the commit introducing this line once we are on Java 8.

  // Antarctica: "Territories south of 60° south latitude"
  private static final double ANTARCTICA_LATITUDE = -60;

  /*
   Some countries are commonly mislabeled and are close to correct, so we want to accommodate them, e.g. Northern
   Ireland mislabeled as Ireland (should be GB). Add comma separated pairs of acceptable country swaps in the
   confused_country_pairs.txt file. Entries will be made in both directions (e.g. IE->GB, GB->IE). Multiple entries per
   country are allowed (e.g. AB,CD and AB,EF).
   This *overrides* the provided country, and includes an issue.
  */
  private static final Map<Country, Set<Country>> CONFUSED_COUNTRIES = Maps.newHashMap();

  static {
    // These can use neater Java 8 lambda expressions once we've upgraded.
    TRANSFORMS.put(Arrays.asList(OccurrenceIssue.PRESUMED_NEGATED_LATITUDE), new Lambda() { @Override public LatLng apply(Double lat, Double lng) { return new LatLng(-1 * lat, lng); }});
    TRANSFORMS.put(Arrays.asList(OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE), new Lambda() { @Override public LatLng apply(Double lat, Double lng) { return new LatLng(lat, -1 * lng); }});
    TRANSFORMS.put(Arrays.asList(OccurrenceIssue.PRESUMED_NEGATED_LATITUDE, OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE), new Lambda() { @Override public LatLng apply(Double lat, Double lng) { return new LatLng(-1 * lat, -1 * lng); }});
    TRANSFORMS.put(Arrays.asList(OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE), new Lambda() { @Override public LatLng apply(Double lat, Double lng) { return new LatLng(lng, lat); }});

    InputStream in = CoordinateInterpreter.class.getClassLoader().getResourceAsStream(CONFUSED_COUNTRY_FILE);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    try {
      String nextLine;
      while ((nextLine = reader.readLine()) != null) {
        if (!nextLine.startsWith("#")) {
          String[] countries = nextLine.split(",");
          String countryA = countries[0].trim().toUpperCase();
          String countryB = countries[1].trim().toUpperCase();
          LOG.info("Adding [{}][{}] pair to confused country matches.", countryA, countryB);
          addConfusedCountry(countryA, countryB);
          addConfusedCountry(countryB, countryA);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't read [" + CONFUSED_COUNTRY_FILE + "] - aborting", e);
    } finally {
      try {
        reader.close();
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOG.warn("Couldn't close [{}] - continuing anyway", CONFUSED_COUNTRY_FILE, e);
      }
    }
  }

  private static void addConfusedCountry(String countryA, String countryB) {
    Country cA = Country.fromIsoCode(countryA);
    if (!CONFUSED_COUNTRIES.containsKey(cA)) {
      CONFUSED_COUNTRIES.put(cA, Sets.<Country>newHashSet());
    }

    Set<Country> confused = CONFUSED_COUNTRIES.get(cA);
    confused.add(cA);
    confused.add(Country.fromIsoCode(countryB));
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

  private OccurrenceParseResult<CoordinateResult> verifyLatLon(OccurrenceParseResult<LatLng> parsedLatLon, final String datum, final Country country) {
    // use original as default
    Country finalCountry = country;
    final Set<OccurrenceIssue> issues = EnumSet.noneOf(OccurrenceIssue.class);

    issues.addAll(parsedLatLon.getIssues());
    if (!parsedLatLon.isSuccessful()) {
      return OccurrenceParseResult.fail(issues);
    }

    // interpret geodetic datum and reproject if needed
    // the reprojection will keep the original values even if it failed with issues
    parsedLatLon = Wgs84Projection
      .reproject(parsedLatLon.getPayload().getLat(), parsedLatLon.getPayload().getLng(), datum);
    issues.addAll(parsedLatLon.getIssues());

    // Find the country/ies that this point is located in.  This can be more than one close to borders.
    List<Country> latLngCountries = getCountryForLatLng(parsedLatLon.getPayload());

    if (country == null) {
      if (!latLngCountries.isEmpty()) {
        // use the first coordinate derived country instead of nothing
        finalCountry = latLngCountries.get(0);
        issues.add(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);
      }
    } else {

      // Check for provided country being in the set of possible countries
      Country matchCountry = matchCountry(country, latLngCountries, issues);

      if (matchCountry != null) {
        // Take the returned country, in case we had a confused match.
        finalCountry = matchCountry;
      } else {
        // Try different transformations on coordinates to see if the resulting lookup would match the specified country.
        LatLng coord = parsedLatLon.getPayload();

        parsedLatLon = null;
        for (Map.Entry<List<OccurrenceIssue>, Lambda> geospatialIssueEntry : TRANSFORMS.entrySet()) {
          Lambda transform = geospatialIssueEntry.getValue();
          LatLng tCoord = transform.apply(coord.getLat(), coord.getLng());
          matchCountry = matchCountry(country, getCountryForLatLng(tCoord), issues);
          if (matchCountry != null) {
            // transformation worked and matches given country!
            // use the changed coords!
            // but don't return the country, we are just showing corrected coordinates and an issue.
            parsedLatLon = OccurrenceParseResult.fail(tCoord, geospatialIssueEntry.getKey());
            break;
          }
        }
        if (parsedLatLon == null) {
          // Transformations failed
          parsedLatLon = OccurrenceParseResult.fail(coord, OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH);
        }

        issues.addAll(parsedLatLon.getIssues());
      }
    }

    if (parsedLatLon.getPayload() == null) {
      // something has gone very wrong
      LOG.warn("Supposed coordinate interpretation success produced no latlng", parsedLatLon);
      return OccurrenceParseResult.fail(issues);
    }

    return OccurrenceParseResult.success(parsedLatLon.getConfidence(),
                               new CoordinateResult(parsedLatLon.getPayload(), finalCountry),  issues);
  }

  /**
   * @return true if the given country (or its close confused neighbours) is one of the potential countries given
   */
  private static Country matchCountry(Country country, Collection<Country> potentialCountries, Set<OccurrenceIssue> issues) {
    // First check for the country in the potential countries
    if (potentialCountries.contains(country)) {
      return country;
    }

    // Then also check with commonly confused neighbours
    Set<Country> confusedCountries = CONFUSED_COUNTRIES.get(country);
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
