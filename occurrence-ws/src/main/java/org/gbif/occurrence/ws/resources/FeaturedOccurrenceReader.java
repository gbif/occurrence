package org.gbif.occurrence.ws.resources;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.api.service.registry.OrganizationService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader that scans the featured occurrence table, and produces the list of the occurrences to feature.
 * Only occurrences with scientific names are featured.
 * Class is public to allow Guice wiring, but can only be used in this package.
 */
public class FeaturedOccurrenceReader {

  private static final byte[] CF = Bytes.toBytes("o");
  private static final byte[] COL = Bytes.toBytes("v");
  private static final Pattern COMMA_PATTERN = Pattern.compile(",");
  private static final Logger LOG = LoggerFactory.getLogger(FeaturedOccurrenceReader.class);
  private static final int PAGE_SIZE = 50;
  // The randomization modulus value used when creating the sampled data. See the README.
  private static final int RANDOMIZATION_MODULUS = 5;

  private final OccurrenceService occurrenceService;
  private final OrganizationService organizationService;
  private final Connection connection;
  private final TableName tableName;
  private final Random random = new Random();
  private final Meter requests = Metrics.newMeter(FeaturedOccurrenceReader.class, "requests", "requests",
                                                  TimeUnit.SECONDS);
  private final Meter missingOccurrences = Metrics.newMeter(FeaturedOccurrenceReader.class, "missingOccurrences",
                                                            "missingOccurrences", TimeUnit.SECONDS);
  private final Meter registryFailures = Metrics.newMeter(FeaturedOccurrenceReader.class, "registryFailures",
                                                          "registryFailures", TimeUnit.SECONDS);

  @Inject
  public FeaturedOccurrenceReader(OccurrenceService occurrenceService,
                                  @Named("featured_table_pool") Connection connection,
                                  @Named("featured_table_name") String tableName,
                                  OrganizationService organizationService) {
    this.occurrenceService = occurrenceService;
    this.connection = connection;
    this.tableName = TableName.valueOf(tableName);
    this.organizationService = organizationService;
  }

  private Result getRow(String key) throws  IOException {
    try (Table htable = connection.getTable(tableName)){
      return htable.get(new Get(Bytes.toBytes(key)));
    }
  }

  private void appendOccurrence(List<Occurrence> results, String key) throws IOException {
    Result row = getRow(key);
    String[] occurrenceIds = COMMA_PATTERN.split(Bytes.toString(row.getValue(CF, COL)));
    int randomIndex = random.nextInt(occurrenceIds.length);
    Occurrence occ = occurrenceService.get(Integer.parseInt(occurrenceIds[randomIndex]));
    if (occ == null) {
      missingOccurrences.mark();
      // keep logging low - issues will become apparent quickly
      LOG.debug("No featured occurrence record found for key: {}", occurrenceIds[randomIndex]);
    } else {
      // filter for only good ones
      if (occ.getScientificName() != null && occ.getDecimalLatitude() != null && occ.getDecimalLongitude() != null
        && !occ.hasSpatialIssue() && occ.getPublishingOrgKey() != null) {
        results.add(occ);
      }
    }
  }


  /**
   * Converts the results into featured occurrences.
   */
  private List<FeaturedOccurrence> asFeatured(List<Occurrence> results) {
    List<FeaturedOccurrence> featured = Lists.newArrayList();
    Map<UUID, Organization> orgCache = Maps.newHashMap(); // reduce registry calls
    for (Occurrence o : results) {
      try {
        Organization org = orgCache.get(o.getPublishingOrgKey());
        if (org == null) {
          org = organizationService.get(o.getPublishingOrgKey());
          if (org != null) {
            orgCache.put(org.getKey(), org);
          } else {
            LOG.warn("Suspicious that registry reports no org[" + o.getPublishingOrgKey() + "] for occurrence["
              + o.getKey() + "]");
            registryFailures.mark(); // not communication, but still erroneous
            continue;
          }
        }
        if (org != null) {
          featured.add(new FeaturedOccurrence(o.getKey(), o.getDecimalLatitude(), o.getDecimalLongitude(),
                                              o.getScientificName(), org.getTitle(), org.getKey(),
                                              o.getLastInterpreted()));
        }
      } catch (Exception e) {
        registryFailures.mark();
        LOG.error("Unable to read organizaton[{}] from registry", o.getPublishingOrgKey(), e);
      }
    }
    return featured;
  }

  /**
   * Generates a collection of points to use.
   * Deliberately package only visible.
   */
  List<FeaturedOccurrence> featuredOccurrences() throws IOException {
    requests.mark();
    List<Occurrence> results = Lists.newArrayList();
    for (int i = 0; i < PAGE_SIZE; i++) {
      // randomly pick a cell, from which we then randomly select an occurrence
      // this gives a reasonable effect,
      String randomKey = random.nextInt(RANDOMIZATION_MODULUS) + ":" + random.nextInt(RANDOMIZATION_MODULUS);
      appendOccurrence(results, randomKey);
    }

    return asFeatured(results);
  }
}
