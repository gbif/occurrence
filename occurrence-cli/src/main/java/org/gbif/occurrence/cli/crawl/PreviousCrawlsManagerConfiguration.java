package org.gbif.occurrence.cli.crawl;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.occurrence.cli.common.HiveJdbcConfiguration;
import org.gbif.occurrence.cli.common.SchedulingConfiguration;

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

  /**
   * This is optional since it can be used with or without scheduling.
   */
  @ParametersDelegate
  @Valid
  public SchedulingConfiguration scheduling = new SchedulingConfiguration();

  @ParametersDelegate
  public RegistryConfiguration registry = new RegistryConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public HiveJdbcConfiguration hive = new HiveJdbcConfiguration();

  @Parameter(names = "--hive-occurrence-table")
  @NotNull
  public String hiveOccurrenceTable;

  @Parameter(names = "--occurrence-ws-url")
  @NotNull
  public String occurrenceWsUrl;

  /**
   * the value is a percentage
   */
  @Parameter(names = "--automatic-deletion-threshold")
  @NotNull
  @Max(100)
  @Valid
  public double automaticRecordDeletionThreshold = 30;

  @Parameter(names = "--delete-message-batch-size")
  @Valid
  public int deleteMessageBatchSize = 100;

  @Parameter(names = "--delete-message-batch-interval")
  @Valid
  public int deleteMessageBatchIntervalMs = 100;

  @Parameter(names = "--dataset-autodeletion-limit")
  @Valid
  public int datasetAutodeletionLimit = 2;

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

  /**
   * Emit delete messages for all occurrence records that are not in the latest crawl for the provided
   * dataset-key (even if below automaticRecordDeletionThreshold).
   */
  @Parameter(names = "--force-delete")
  public boolean forceDelete = false;

  @Parameter(names = "--report-output-filepath")
  public String reportOutputFilepath;

  @Parameter(names = "--display-report")
  public boolean displayReport = false;

  /**
   * Simple container for configuration related to registry.
   */
  public static class RegistryConfiguration {

    @Parameter(names = "--registry-app-key")
    public String appKey;

    @Parameter(names = "--registry-app-secret")
    public String appSecret;

    @Parameter(names = "--registry-username")
    public String username;

    @Parameter(names = "--registry-ws-url")
    @NotNull
    public String wsUrl;
  }

}
