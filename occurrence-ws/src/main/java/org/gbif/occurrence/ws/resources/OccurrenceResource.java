package org.gbif.occurrence.ws.resources;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.metrics.cube.Rollup;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Kingdom;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.occurrence.search.OccurrenceGetByKey;
import org.gbif.ws.server.interceptor.NullToNotFound;
import org.gbif.ws.util.ExtraMediaTypes;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.ws.paths.OccurrencePaths.FRAGMENT_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.VERBATIM_PATH;

/**
 * Occurrence resource, the verbatim sub resource, and occurrence metrics.
 */
@Path(OCCURRENCE_PATH)
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
public class OccurrenceResource {

  @VisibleForTesting
  public static final String ANNOSYS_PATH = "annosys";

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceResource.class);

  private final OccurrenceService occurrenceService;
  private final OccurrenceSearchService occurrenceSearchService;

  private final OccurrenceGetByKey occurrenceGetByKey;

  @Inject
  public OccurrenceResource(
    OccurrenceService occurrenceService,
    OccurrenceSearchService occurrenceSearchService,
    OccurrenceGetByKey occurrenceGetByKey
  ) {
    this.occurrenceService = occurrenceService;
    this.occurrenceSearchService = occurrenceSearchService;
    this.occurrenceGetByKey = occurrenceGetByKey;
  }

  /**
   * This retrieves a single Occurrence detail by its key from HBase.
   *
   * @param key Occurrence key
   * @return requested Occurrence or null if none could be found
   */
  @GET
  @Path("/{id}")
  @NullToNotFound
  public Occurrence get(@PathParam("id") Long key) {
    LOG.debug("Request Occurrence [{}]:", key);
    return occurrenceGetByKey.get(key);
  }

  /**
   * This retrieves a single occurrence fragment in its raw form as a string.
   *
   * @param key The Occurrence key
   * @return requested occurrence fragment or null if none could be found
   */
  @GET
  @Path("/{key}/" + FRAGMENT_PATH)
  @NullToNotFound
  public String getFragment(@PathParam("key") Long key) {
    LOG.debug("Request occurrence fragment [{}]:", key);
    return occurrenceService.getFragment(key);
  }

  /**
   * This retrieves a single VerbatimOccurrence detail by its key from HBase and transforms it into the API
   * version which uses Maps.
   *
   * @param key The Occurrence key
   * @return requested VerbatimOccurrence or null if none could be found
   */
  @GET
  @Path("/{key}/" + VERBATIM_PATH)
  @NullToNotFound
  public VerbatimOccurrence getVerbatim(@PathParam("key") Long key) {
    LOG.debug("Request VerbatimOccurrence [{}]:", key);
    return occurrenceGetByKey.getVerbatim(key);
  }

  /**
   * Removed API call, which supported a stream of featured occurrences on the old GBIF.org homepage.
   * @return An empty list.
   */
  @GET
  @Path("featured")
  @Deprecated
  public List<Object> getFeaturedOccurrences() {
    LOG.warn("Featured occurrences have been removed.");
    return Lists.newArrayList();
  }

  /**
   * This method is implemented specifically to support Annosys and is not advertised or
   * documented in the public API.  <em>It may be removed at any time without notice</em>.
   *
   * @param key
   * @return
   */
  @GET
  @Path(ANNOSYS_PATH + "/{key}")
  @NullToNotFound
  @Produces(MediaType.APPLICATION_XML)
  public Occurrence getAnnosysOccurrence(@PathParam("key") Long key) {
    LOG.debug("Request Annosys occurrence [{}]:", key);
    return occurrenceGetByKey.get(key);
  }

  /**
   * This method is implemented specifically to support Annosys and is not advertised or
   * documented in the public API.  <em>It may be removed at any time without notice</em>.
   *
   * @param key
   * @return
   */
  @GET
  @Path(ANNOSYS_PATH + "/{key}/" + VERBATIM_PATH)
  @NullToNotFound
  @Produces(MediaType.APPLICATION_XML)
  public VerbatimOccurrence getAnnosysVerbatim(@PathParam("key") Long key) {
    LOG.debug("Request Annosys verbatim occurrence [{}]:", key);
    return occurrenceService.getVerbatim(key);
  }

  /*
   * The following methods implement the Metrics APIs.  These used to be served using the
   * <a href="https://github.com/gbif/metrics">Metrics project</a>, but with SOLR Cloud we can now use SOLR directly.
   */

  /**
   * Looks up an addressable count from the cube.
   */
  @GET
  @Path("/count")
  public Long count(
    @QueryParam("basisOfRecord") BasisOfRecord basisOfRecord,
    @QueryParam("country") String countryIsoCode,
    @QueryParam("datasetKey") UUID datasetKey,
    @QueryParam("isGeoreferenced") Boolean isGeoreferenced,
    @QueryParam("issue") OccurrenceIssue occurrenceIssue,
    @QueryParam("protocol") EndpointType endpointType,
    @QueryParam("publishingCountry") String publishingCountryIsoCode,
    @QueryParam("taxonKey") Integer taxonKey,
    @QueryParam("typeStatus") TypeStatus typeStatus,
    @QueryParam("year") Integer year
  ) {
    OccurrenceSearchRequest osr = new OccurrenceSearchRequest();
    osr.setLimit(0);

    if (basisOfRecord != null) osr.addBasisOfRecordFilter(basisOfRecord);
    if (countryIsoCode != null) osr.addCountryFilter(Country.fromIsoCode(countryIsoCode));
    if (datasetKey != null) osr.addDatasetKeyFilter(datasetKey);
    if (occurrenceIssue != null) osr.addIssueFilter(occurrenceIssue);
    if (publishingCountryIsoCode != null) osr.addPublishingCountryFilter(Country.fromIsoCode(publishingCountryIsoCode));
    if (endpointType != null) osr.addParameter(OccurrenceSearchParameter.PROTOCOL, endpointType);
    if (taxonKey != null) osr.addTaxonKeyFilter(taxonKey);
    if (typeStatus != null) osr.addTypeStatusFilter(typeStatus);
    if (year != null) osr.addYearFilter(year);

    // Georeferenced is different from hasCoordinate and includes a no geospatial issue check.
    if (isGeoreferenced != null) {
      if (isGeoreferenced) {
        osr.addHasCoordinateFilter(true);
        osr.addSpatialIssueFilter(false);
      } else {
        // "Not georeferenced" means either no coordinates, or coordinates but an issue.

        Long count = 0L;

        // No coordinates
        osr.addHasCoordinateFilter(false);
        count += occurrenceSearchService.search(osr).getCount();
        osr.getParameters().removeAll(OccurrenceSearchParameter.HAS_COORDINATE);

        // Has coordinates but with issues
        osr.addHasCoordinateFilter(true);
        osr.addSpatialIssueFilter(true);
        count += occurrenceSearchService.search(osr).getCount();

        return count;
      }
    }

    return occurrenceSearchService.search(osr).getCount();
  }

  @GET
  @Path("/counts/basisOfRecord")
  public Map<BasisOfRecord, Long> getBasisOfRecordCounts() {
    OccurrenceSearchRequest osr = osr(OccurrenceSearchParameter.BASIS_OF_RECORD);

    return count(osr, f -> BasisOfRecord.valueOf(f));
  }

  @GET
  @Path("/counts/countries")
  public Map<Country, Long> getCountries(@QueryParam("publishingCountry") String publishingCountryIso) {
    if (publishingCountryIso == null) {
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
        .entity("publishingCountry parameter is required")
        .build());
    }

    OccurrenceSearchRequest osr = osr(OccurrenceSearchParameter.COUNTRY);

    osr.addPublishingCountryFilter(Country.fromIsoCode(publishingCountryIso));

    return count(osr, f -> Country.fromIsoCode(f));
  }

  @GET
  @Path("/counts/datasets")
  public Map<UUID, Long> getDatasets(@QueryParam("country") String countryIso,
                                     @QueryParam("nubKey") Integer nubKey, @QueryParam("taxonKey") Integer taxonKey) {
    OccurrenceSearchRequest osr = osr(OccurrenceSearchParameter.DATASET_KEY);

    if (countryIso != null) {
      osr.addCountryFilter(Country.fromIsoCode(countryIso));
    }

    // legacy parameter is nubKey, but API docs specified taxonKey so we simply allow both
    if (nubKey != null || taxonKey != null) {
      osr.addTaxonKeyFilter(nubKey == null ? taxonKey : nubKey);
    }

    return count(osr, f -> UUID.fromString(f));
  }

  @GET
  @Path("/counts/kingdom")
  public Map<Kingdom, Long> getKingdomCounts() {
    OccurrenceSearchRequest osr = osr(OccurrenceSearchParameter.KINGDOM_KEY);

    return count(osr, f -> Kingdom.byNubUsageKey(Integer.parseInt(f)));
  }

  @GET
  @Path("/counts/publishingCountries")
  public Map<Country, Long> getPublishingCountries(@QueryParam("country") String countryIso) {
    if (countryIso == null) {
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
        .entity("country parameter is required")
        .build());
    }

    OccurrenceSearchRequest osr = osr(OccurrenceSearchParameter.PUBLISHING_COUNTRY);

    osr.addCountryFilter(Country.fromIsoCode(countryIso));

    return count(osr, f -> Country.fromIsoCode(f));
  }

  /**
   * @return The public API schema
   */
  @GET
  @Path("/count/schema")
  public List<Rollup> getSchema() {
    // External Occurrence cube definition
    return org.gbif.api.model.metrics.cube.OccurrenceCube.ROLLUPS;
  }

  @VisibleForTesting
  protected static Range<Integer> parseYearRange(String year) {
    final int now = 1901 + new Date().getYear();
    if (Strings.isNullOrEmpty(year)) {
      // return all years between 1500 and now
      return Range.open(1500, now);
    }
    try {
      Range<Integer> result = null;
      String[] years = year.split(",");
      if (years.length == 1) {
        result = Range.open(Integer.parseInt(years[0].trim()), now);

      } else if (years.length == 2) {
        result = Range.open(Integer.parseInt(years[0].trim()), Integer.parseInt(years[1].trim()));
      }

      // verify upper and lower bounds are sensible
      if (result == null || result.lowerEndpoint().intValue() < 1000 || result.upperEndpoint().intValue() > now) {
        throw new IllegalArgumentException("Valid year range between 1000 and now expected, separated by a comma");
      }
      return result;

    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
        .entity("Parameter "+ year +" is not a valid year range")
        .build());
    }
  }

  @GET
  @Path("/counts/year")
  public Map<Integer, Long> getYearCounts(@QueryParam("year") String year) {
    ImmutableSortedMap.Builder<Integer, Long> distribution = ImmutableSortedMap.naturalOrder();

    OccurrenceSearchRequest osr = osr(OccurrenceSearchParameter.YEAR);
    osr.addParameter(OccurrenceSearchParameter.YEAR, year);

    distribution.putAll(count(osr, Integer::parseInt));

    return distribution.build();
  }


  private OccurrenceSearchRequest osr(OccurrenceSearchParameter facetParameter) {
    OccurrenceSearchRequest osr = new OccurrenceSearchRequest();

    osr.addFacets(facetParameter);
    osr.setFacetLimit(100000);
    osr.setLimit(0);

    return osr;
  }

  private <T extends Comparable> Map<T, Long> count(OccurrenceSearchRequest osr, Function<String, T> convert) {
    SearchResponse<Occurrence, OccurrenceSearchParameter> response = occurrenceSearchService.search(osr);

    Map<T, Long> results = new HashMap<>();

    // Defensive coding: check we get the facet we expect, and ignore any others.
    OccurrenceSearchParameter expectedFacet = osr.getFacets().iterator().next();
    for (Facet<OccurrenceSearchParameter> f : response.getFacets()) {
      if (f.getField() == expectedFacet) {
        for (Facet.Count c : f.getCounts()) {
          results.put(convert.apply(c.getName()), c.getCount());
        }
      }
    }

    return sortDescendingValues(results);
  }

  @VisibleForTesting
  <K extends Comparable> Map<K, Long> sortDescendingValues(Map<K, Long> source) {
    Ordering<Map.Entry<K, Long>> valueOrder = Ordering.natural().onResultOf((Map.Entry<K, Long> entry) -> entry.getValue()).reverse();
    Ordering<Map.Entry<K, Long>> keyOrder = Ordering.natural().onResultOf((Map.Entry<K, Long> entry) -> entry.getKey());

    ImmutableMap.Builder<K, Long> builder = ImmutableMap.builder();
    // we need a compound ordering to guarantee stable order with identical values

    for (Map.Entry<K, Long> entry : valueOrder.compound(keyOrder).sortedCopy(source.entrySet())) {
      builder.put(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

}
