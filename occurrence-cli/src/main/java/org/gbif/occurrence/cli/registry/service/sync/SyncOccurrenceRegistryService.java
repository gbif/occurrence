package org.gbif.occurrence.cli.registry.service.sync;

import org.gbif.occurrence.cli.registry.sync.AbstractOccurrenceRegistryMapper;
import org.gbif.occurrence.cli.registry.sync.OccurrenceRegistryMapper;
import org.gbif.occurrence.cli.registry.sync.OccurrenceScanMapper;
import org.gbif.occurrence.cli.registry.sync.SyncCommon;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service will create and send a Map/Reduce job to the cluster to update occurrence records in HBase
 * to match the value in the Registry (see {@link OccurrenceRegistryMapper}).
 *
 */
public class SyncOccurrenceRegistryService {

  private static final Logger LOG = LoggerFactory.getLogger(SyncOccurrenceRegistryService.class);

  private static final int SCAN_CACHING = 200;

  private final SyncOccurrenceRegistryConfiguration configuration;
  private final Properties properties;

  public SyncOccurrenceRegistryService(SyncOccurrenceRegistryConfiguration configuration) {
    this.configuration = configuration;
    properties = translateConfigurationIntoProperties(configuration);
  }

  /**
   * Creates and submit a MapReduce job to the cluster using {@link OccurrenceScanMapper} as a mapper.
   *
   */
  public void doRun() {

    Optional<UUID> datasetKey = Optional.ofNullable(configuration.datasetKey != null ? UUID.fromString(configuration.datasetKey ) : null);
    Optional<Long> lastUpdatedAfterMs = Optional.ofNullable(configuration.since);

    //create the HBase config here since hbase-site.xml is (at least should) be in our classpath.
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.client.scanner.timeout.period", configuration.hbase.timeoutMs);
    conf.set("hbase.rpc.timeout", configuration.hbase.timeoutMs);

    // add all props to job context for use by the OccurrenceRegistryMapper when it no longer has access to our classpath
    for (Object key : properties.keySet()) {
      String stringKey = (String)key;
      conf.set(stringKey, properties.getProperty(stringKey));
    }

    String jobTitle = "Registry-Occurrence Sync on HBase table " + configuration.hbase.occurrenceTable;

    jobTitle += datasetKey.map( key -> " for dataset " + key).orElse("");
    jobTitle += lastUpdatedAfterMs.map( ms -> " lastInterpreted since " + ms + " ms").orElse("");

    try {
      Job job = Job.getInstance(conf, jobTitle);
      job.setJarByClass(OccurrenceRegistryMapper.class);
      job.setOutputFormatClass(NullOutputFormat.class);

      job.setNumReduceTasks(0);
      job.getConfiguration().set("mapreduce.map.speculative", "false");
      job.getConfiguration().set("mapreduce.reduce.speculative", "false");
      job.getConfiguration().set("mapreduce.client.submit.file.replication", "3");
      job.getConfiguration().set("mapreduce.task.classpath.user.precedence", "true");
      job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
      job.getConfiguration().set("mapreduce.map.memory.mb", configuration.mapReduce.mapMemoryMb);
      job.getConfiguration().set("mapreduce.map.java.opts", configuration.mapReduce.mapJavaOpts);
      job.getConfiguration().set("mapred.job.queue.name", configuration.mapReduce.queueName);

      Scan scan = buildScan(datasetKey, lastUpdatedAfterMs);

      // NOTE: addDependencyJars must be false or you'll see it trying to load hdfs://c1n1/home/user/app/lib/occurrence-cli.jar
      TableMapReduceUtil
              .initTableMapperJob(configuration.hbase.occurrenceTable, scan, OccurrenceRegistryMapper.class,
                      ImmutableBytesWritable.class, NullWritable.class, job, false);
      job.waitForCompletion(true);
    } catch (Exception e) {
      LOG.error("Could not start m/r job, aborting", e);
    }

    LOG.info("Finished running m/r sync for dataset [{}]", datasetKey);
  }

  /**
   * Translate the {@link SyncOccurrenceRegistryConfiguration} into a Properties object that is expected by
   * {@link OccurrenceRegistryMapper}.
   *
   * @param configuration
   * @return
   */
  private static Properties translateConfigurationIntoProperties(SyncOccurrenceRegistryConfiguration configuration) {

    Properties props = new Properties();

    //messaging related properties
    props.setProperty("sync.postalservice.username", configuration.messaging.username);
    props.setProperty("sync.postalservice.password", configuration.messaging.password);
    props.setProperty("sync.postalservice.virtualhost", configuration.messaging.virtualHost);
    props.setProperty("sync.postalservice.hostname", configuration.messaging.host);
    props.setProperty("sync.postalservice.port", Integer.toString(configuration.messaging.port));

    props.setProperty(AbstractOccurrenceRegistryMapper.PROP_OCCURRENCE_TABLE_NAME_KEY, configuration.hbase.occurrenceTable);
    props.setProperty(SyncCommon.REG_WS_PROPS_KEY,  configuration.registryWsUrl);

    return props;
  }

  /**
   * Build a {@link Scan} based on Optional filters. If no filter is provided the Scan will scan
   * the entire table.
   *
   * @param datasetKey
   * @param lastUpdatedAfterMs
   * @return
   */
  private static Scan buildScan(Optional<UUID> datasetKey, Optional<Long> lastUpdatedAfterMs) {

    Scan scan = new Scan();
    scan.addColumn(SyncCommon.OCC_CF, SyncCommon.DK_COL); //datasetKey
    scan.addColumn(SyncCommon.OCC_CF, SyncCommon.HC_COL); //publishingCountry
    scan.addColumn(SyncCommon.OCC_CF, SyncCommon.OOK_COL); //publishingOrgKey
    scan.addColumn(SyncCommon.OCC_CF, SyncCommon.CI_COL); //crawlId
    scan.addColumn(SyncCommon.OCC_CF, SyncCommon.LI_COL); //lastInterpreted
    scan.addColumn(SyncCommon.OCC_CF, SyncCommon.LICENSE_COL);
    scan.setCaching(SCAN_CACHING);
    scan.setCacheBlocks(false); // don't set to true for MR jobs (from HBase MapReduce Examples)

    FilterList filterList = new FilterList();
    datasetKey.ifPresent(key -> filterList.addFilter(
            new SingleColumnValueFilter(SyncCommon.OCC_CF, SyncCommon.DK_COL, CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes(key.toString()))));

    lastUpdatedAfterMs.ifPresent(ms -> filterList.addFilter(
            new SingleColumnValueFilter(SyncCommon.OCC_CF, SyncCommon.LI_COL, CompareFilter.CompareOp.GREATER,
                    Bytes.toBytes(ms))));

    if (datasetKey.isPresent() || lastUpdatedAfterMs.isPresent()) {
      scan.setFilter(filterList.getFilters().size() == 1 ? filterList.getFilters().get(0) :
              filterList);
    }
    return scan;
  }

}
