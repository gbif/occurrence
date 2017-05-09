package org.gbif.occurrence.cli.crawl;

import org.gbif.occurrence.cli.common.HiveJdbcConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

/**
 * Main configuration for generation crawls report.
 */
public class CrawlsReportGeneratorConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public HiveJdbcConfiguration hive = new HiveJdbcConfiguration();

  @Parameter(names = "--hive-occurrence-table")
  @NotNull
  public String hiveOccurrenceTable;

  @Parameter(names = "--report-location")
  @NotNull
  public String reportLocation;
}

