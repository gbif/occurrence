package org.gbif.occurrence.interpreters;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceValidationRule;
import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.geospatial.GeospatialParseUtils;
import org.gbif.common.parsers.geospatial.LatLngIssue;
import org.gbif.geocode.api.model.Location;
import org.gbif.occurrence.interpreters.result.CoordinateInterpretationResult;
import org.gbif.occurrence.interpreters.util.RetryingWebserviceClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.MultivaluedMap;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache.ApacheHttpClient;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Attempts to parse given String lat and long into Doubles, and compares the given country (if any) to a reverse
 * lookup of the parsed coordinates. If no country was given and the lookup produced something, that looked up result
 * is returned. If the lookup result and passed in country don't match, a "GeospatialIssue" is noted.
 */
public class CoordinateInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinateInterpreter.class);

  // if the WS is not responding, we drop into a retry count
  private static final int NUM_RETRIES = 5;
  private static final int RETRY_PERIOD_MSEC = 2000;

  private static final String WEB_SERVICE_URL;
  private static final String WEB_SERVICE_URL_PROPERTY = "occurrence.geo.ws.url";

  private static final String FUZZY_COUNTRY_FILE;
  private static final String FUZZY_COUNTRY_FILE_PROPERTY = "occurrence.geo.fuzzycountryfile";

  // The repetitive nature of our data encourages use of a light cache to reduce WS load
  private static final LoadingCache<WebResource, Location[]> CACHE =
    CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(1, TimeUnit.MINUTES)
      .build(RetryingWebserviceClient.newInstance(Location[].class, 10, 2000));

  private static final WebResource RESOURCE;

  private static final Map<OccurrenceValidationRule, Integer[]> TRANSFORMS =
    new EnumMap<OccurrenceValidationRule, Integer[]>(OccurrenceValidationRule.class);

  /*
   Some countries are commonly mislabeled and are close to correct, so we want to accommodate them, e.g. Northern
   Ireland mislabeled as Ireland (should be GB). Add comma separated pairs of acceptable country swaps in the
   fuzzy_country_pairs.txt file. Entries will be made in both directions (e.g. IE->GB, GB->IE). Multiple entries per
   country are allowed (e.g. AB,CD and AB,EF). This is only for setting of geospatial flags.
  */
  private static final Map<Country, Set<Country>> FUZZY_COUNTRIES = Maps.newHashMap();

  static {
    TRANSFORMS.put(OccurrenceValidationRule.PRESUMED_NEGATED_LATITUDE, new Integer[] {-1, 1});
    TRANSFORMS.put(OccurrenceValidationRule.PRESUMED_NEGATED_LONGITUDE, new Integer[] {1, -1});
    TRANSFORMS.put(OccurrenceValidationRule.PRESUMED_SWAPPED_COORDINATE, new Integer[] {-1, -1});

    try {
      InputStream is =
        NubLookupInterpreter.class.getClassLoader().getResourceAsStream("occurrence-interpreter.properties");
      if (is == null) {
        throw new RuntimeException("Can't load properties file [occurrence-interpreter.properties]");
      }
      try {
        Properties props = new Properties();
        props.load(is);
        WEB_SERVICE_URL = props.getProperty(WEB_SERVICE_URL_PROPERTY);
        FUZZY_COUNTRY_FILE = props.getProperty(FUZZY_COUNTRY_FILE_PROPERTY);
      } finally {
        is.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't load properties file [occurrence-interpreter.properties]", e);
    }

    ClientConfig cc = new DefaultClientConfig();
    cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);

    // We need to manually register it here because we're building an assembly and Jackson can't find it automatically
    cc.getClasses().add(JacksonJsonProvider.class);
    Client client = ApacheHttpClient.create(cc);
    RESOURCE = client.resource(WEB_SERVICE_URL);

    if (FUZZY_COUNTRY_FILE == null) {
      LOG.warn("Fuzzy country file property [{}] not set, so not loading any fuzzy country pairs.",
        FUZZY_COUNTRY_FILE_PROPERTY);
    } else {
      InputStream in = CoordinateInterpreter.class.getClassLoader().getResourceAsStream(FUZZY_COUNTRY_FILE);
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      try {
        String nextLine;
        while ((nextLine = reader.readLine()) != null) {
          if (!nextLine.startsWith("#")) {
            String[] countries = nextLine.split(",");
            String countryA = countries[0].trim().toUpperCase();
            String countryB = countries[1].trim().toUpperCase();
            LOG.info("Adding [{}][{}] pair to fuzzy country matches.", countryA, countryB);
            addFuzzyCountry(countryA, countryB);
            addFuzzyCountry(countryB, countryA);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Can't read [" + FUZZY_COUNTRY_FILE + "] - aborting", e);
      } finally {
        try {
          reader.close();
          if (in != null) {
            in.close();
          }
        } catch (IOException e) {
          LOG.warn("Couldn't close [{}] - continuing anyway", FUZZY_COUNTRY_FILE, e);
        }
      }
    }
  }

  private static void addFuzzyCountry(String countryA, String countryB) {
    Country cA = Country.fromIsoCode(countryA);
    if (!FUZZY_COUNTRIES.containsKey(cA)) {
      FUZZY_COUNTRIES.put(cA, Sets.<Country>newHashSet());
    }

    Set<Country> fuzzy = FUZZY_COUNTRIES.get(cA);
    fuzzy.add(cA);
    fuzzy.add(Country.fromIsoCode(countryB));
  }

  /**
   * Should not be instantiated.
   */
  private CoordinateInterpreter() {
  }

  /**
   * Attempts to convert the given lat and long into Doubles, and the given country string into an ISO country code.
   *
   * @param latitude    decimal latitude as string
   * @param longitude   decimal longitude as string
   * @param country     country as interpreted to sanity check coordinate
   *
   * @return the latitude and longitude as doubles, the country as an ISO code, and a "geospatial issue" flag if any
   *         known errors were encountered in the interpretation (e.g. lat/lng reversed), or all fields set to null if
   *         latitude
   *         or longitude are null
   */
  public static CoordinateInterpretationResult interpretCoordinates(String latitude, String longitude, final Country country) {
    if (latitude == null || longitude == null) return new CoordinateInterpretationResult();

    ParseResult<LatLngIssue> parseResult = GeospatialParseUtils.parseLatLng(latitude, longitude);

    // round to 5 decimals (~1m precision) since no way we're getting anything legitimately more precise
    Double lat = parseResult.getPayload() == null || parseResult.getPayload().getLat() == null ? null
      : Math.round(parseResult.getPayload().getLat() * Math.pow(10, 5)) / Math.pow(10, 5);
    Double lng = parseResult.getPayload() == null || parseResult.getPayload().getLng() == null ? null
      : Math.round(parseResult.getPayload().getLng() * Math.pow(10, 5)) / Math.pow(10, 5);

    Country finalCountry = country;

    // the utils do a basic sanity check - even if it suggests success, we have to check that the lat/long
    // actually falls in the country given in the record. If it doesn't, try common mistakes and note the issue
    if (parseResult.getStatus() == ParseResult.STATUS.SUCCESS) {
      List<Country> latLngCountries = getCountryForLatLng(lat, lng);
      if (finalCountry == null) {
        // we have nothing to say about coord accuracy, but can try to fetch the right country
        if (!latLngCountries.isEmpty()) {
          finalCountry = latLngCountries.get(0);
        }

      } else if (matchCountry(country, latLngCountries)) {
        // in cases where fuzzy match we want to use the lookup value, not the fuzzy one
        finalCountry = latLngCountries.get(0);

      } else {
        boolean match = false;
        for (Map.Entry<OccurrenceValidationRule, Integer[]> geospatialIssueEntry : TRANSFORMS.entrySet()) {
          Integer[] transform = geospatialIssueEntry.getValue();
          if (matchCountry(country, getCountryForLatLng(lat * transform[0], lng * transform[1]))) {
            parseResult = ParseResult.fail(new LatLngIssue(lat, lng, geospatialIssueEntry.getKey()));
            match = true;
            break;
          }
        }

        // if we made it here no transforms worked and the point is either in international waters or really weird
        parseResult = match ? parseResult
          : ParseResult.fail(new LatLngIssue(lat, lng, OccurrenceValidationRule.COUNTRY_COORDINATE_MISMATCH));
      }
    }

    CoordinateInterpretationResult result;
    if (parseResult.getStatus() == ParseResult.STATUS.SUCCESS || parseResult.getStatus() == ParseResult.STATUS.FAIL) {
      result = new CoordinateInterpretationResult(lat, lng, finalCountry);
      if (parseResult.getPayload().getIssue() != null) {
        result.setValidationRule(parseResult.getPayload().getIssue(), true);
      }
    } else {
      result = new CoordinateInterpretationResult();
    }

    return result;
  }

  private static boolean matchCountry(Country country, Collection<Country> potentialCountries) {
    Set<Country> fuzzyCountries = FUZZY_COUNTRIES.get(country);
    if (fuzzyCountries == null) {
      fuzzyCountries = Sets.newHashSet();
      fuzzyCountries.add(country);
      FUZZY_COUNTRIES.put(country, fuzzyCountries);
    }

    boolean match = false;
    for (Country pCountry : potentialCountries) {
      if (fuzzyCountries.contains(pCountry)) {
        match = true;
        break;
      }
    }

    return match;
  }

  /**
   * It's theoretically possible that the webservice could respond with more than one country, though it's not
   * known under what conditions that might happen.
   */
  private static List<Country> getCountryForLatLng(Double lat, Double lng) {
    List<Country> countries = Lists.newArrayList();

    MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
    queryParams.add("lat", lat.toString());
    queryParams.add("lng", lng.toString());

    for (int i = 0; i < NUM_RETRIES; i++) {
      LOG.debug("Attempt [{}] to lookup lat [{}] lng [{}]", i, lat, lng);
      try {
        Location[] lookups = CACHE.get(RESOURCE.queryParams(queryParams));
        if (lookups != null && lookups.length > 0) {
          LOG.debug("Successfully retrieved [{}] locations for lat [{}] lng [{}]", lookups.length, lat, lng);
          for (Location loc : lookups) {
            if (loc.getIsoCountryCode2Digit() != null) {
              countries.add(Country.fromIsoCode(loc.getIsoCountryCode2Digit()));
            }
          }
        }
        break; // from retry loop
      } catch (ExecutionException e) {
        // Log the error
        StringBuilder sb = new StringBuilder("Failed WS call with: ");
        for (Map.Entry<String, List<String>> en : queryParams.entrySet()) {
          sb.append(en.getKey()).append('=');
          for (String s : en.getValue()) {
            sb.append(s);
          }
          sb.append('&');
        }
        LOG.error(sb.toString(), e);

        // have we exhausted our attempts?
        if (i >= NUM_RETRIES) {
          throw new RuntimeException(e);
        }

        try {
          Thread.sleep(RETRY_PERIOD_MSEC);
        } catch (InterruptedException e1) {
        }
      }
    } // retry loop

    return countries;
  }
}
