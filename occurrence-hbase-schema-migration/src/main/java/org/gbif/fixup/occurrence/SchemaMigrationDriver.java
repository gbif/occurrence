package org.gbif.fixup.occurrence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Launches a process to populate an HBase table with content from the snapshot of an existing one, modifying the
 * data structure and adding table salting to the key.
 * Requires that a snapshot of the source table exists and the target table exists which should be presplit.
 */
public class SchemaMigrationDriver extends Configured implements Tool {

  public static void main(String... args) throws Exception {
    int status = ToolRunner.run(new SchemaMigrationDriver(), args);
    System.exit(status);
  }

  private static Job createSubmittableJob(Configuration conf, String snapshotName, String targetTableName, int modulo, Path tmpPath)
      throws IOException {

    Job job = Job.getInstance(conf, "HBase occurrence schema migration");
    job.getConfiguration().setInt("modulo", modulo);
    job.setJarByClass(ReformatRecordMapper.class) ;

    Scan scan = new Scan();
    scan.setCaching(1000);
    // If we only want a few fields, we can improve performance by declaring them here in the scan

    Path restore = tmpPath.suffix("-restore");

    TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName, scan, ReformatRecordMapper.class,
      ImmutableBytesWritable.class, Put.class, job,true, restore);
    FileOutputFormat.setOutputPath(job, tmpPath);

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      TableName tableName = TableName.valueOf(targetTableName);
      Table table = connection.getTable(tableName);
      RegionLocator regionLocator = connection.getRegionLocator(tableName);
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
    }

    return job;
  }

  @Override
  public int run(String[] args) throws Exception {

    List<String> otherArgs = new ArrayList<>(Arrays.asList(new GenericOptionsParser(getConf(), args).getRemainingArgs()));
    if (otherArgs.size() < 5) {
      System.err.println("Wrong number of arguments: " + otherArgs.size());
      System.err.println("Need five arguments '-zk',  '-snapshot', '-target', '-modulo' and '-tmpPath'");
      return -1;
    }

    String zk = StringUtils.popOptionWithArgument("-zk", otherArgs);
    String snapshot = StringUtils.popOptionWithArgument("-snapshot", otherArgs);
    String dest = StringUtils.popOptionWithArgument("-target", otherArgs);
    int modulo = Integer.valueOf(StringUtils.popOptionWithArgument("-modulo", otherArgs));
    Path tmpPath = new Path(StringUtils.popOptionWithArgument("-tmpPath", otherArgs));

    Configuration conf = HBaseConfiguration.create(getConf());
    conf.set("hbase.zookeeper.quorum", zk);
    setConf(conf);

    Job job = createSubmittableJob(conf, snapshot, dest, modulo, tmpPath);
    boolean success = job.waitForCompletion(true);

    doBulkLoad(dest, tmpPath);

    return success ? 0 : 1;
  }

  private void doBulkLoad(String tableNameString, Path tmpPath) throws Exception {
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
    try (Connection connection = ConnectionFactory.createConnection(getConf()); Admin admin = connection.getAdmin()) {
      TableName tableName = TableName.valueOf(tableNameString);
      Table table = connection.getTable(tableName);
      RegionLocator regionLocator = connection.getRegionLocator(tableName);
      loader.doBulkLoad(tmpPath, admin, table, regionLocator);
    }
  }
}
