package org.gbif.occurrence.cli.crawl;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.occurrence.cli.common.HiveJdbcConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

/**
 * Configurations related to {@link PreviousCrawlsManagerCommand}.
 */
class PreviousCrawlsManagerConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--registry-ws-url")
  @NotNull
  public String registryWsUrl;

  @ParametersDelegate
  @Valid
  @NotNull
  public HiveJdbcConfiguration hive = new HiveJdbcConfiguration();

  @Parameter(names = "--hive-occurrence-table")
  @NotNull
  public String hiveOccurrenceTable;

  /**
   * the value is a percentage
   */
  @Parameter(names = "--automatic-deletion-threshold")
  @NotNull
  @Max(100)
  @Valid
  public double automaticRecordDeletionThreshold = 30;

  /**
   * optional config normally received on command line
   */
  @Parameter(names = "--dataset-key")
  public String datasetKey;

  /**
   * Emit delete messages for all occurrence records that are not in the latest crawl for the provided
   * dataset-key (if below automaticRecordDeletionThreshold).
   */
  @Parameter(names = "--delete")
  public boolean delete = false;

  @Parameter(names = "--report-location")
  public String reportLocation;

  @Parameter(names = "--display-report")
  public boolean displayReport = false;

}
