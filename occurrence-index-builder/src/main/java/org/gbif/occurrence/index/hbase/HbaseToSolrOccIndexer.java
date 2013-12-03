package org.gbif.occurrence.index.hbase;


import org.gbif.occurrence.index.hadoop.SolrDocumentConverter;
import org.gbif.occurrence.index.hadoop.SolrOutputFormat;
import org.gbif.occurrence.index.hadoop.SolrReducer;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HbaseToSolrOccIndexer extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new HbaseToSolrOccIndexer(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
        .println("Usage: SolrOccIndexer  -solr <solrHome> -htable <hbaseTable>[-shards NNN] [-compress_output]");
      System.err.println("\tinputDir\tinput directory(-ies) containing occurrence files");
      System.err.println("\toutputDir\toutput directory containing Solr indexes.");
      System.err.println("\tsolr <solrHome>\tlocal directory containing Solr conf/ and lib/");
      System.err.println("\tshards NNN\tset the number of output shards to NNN");
      System.err.println("\t\t(default: the default number of reduce tasks)");
      System.err.println("\tcompress_output\tto compress the output of the reducer tasks (create .zip file)");
      return -1;
    }
    Job job = new Job(getConf());
    job.setJarByClass(HbaseToSolrOccIndexer.class);
    job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
    job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
    job.getConfiguration().set("mapred.task.timeout", "900000"); // 10 mins
    job.getConfiguration().set("hbase.regionserver.lease.period", "900000");
    job.getConfiguration().set("mapred.compress.map.output", "true");
    job.getConfiguration().set("mapred.output.compress", "true");


    int shards = 1;
    boolean compressOutput = false;
    String solrHome = null;
    Path out = new Path(args[0]);
    String inputTable = null;
    for (int i = 1; i < args.length; i++) {
      if (args[i] == null) {
        continue;
      }
      if (args[i].equals("-shards")) {
        shards = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-compress_output")) {
        compressOutput = true;
      } else if (args[i].equals("-solr")) {
        solrHome = args[++i];
        continue;
      } else if (args[i].equals("-htable")) {
        inputTable = args[++i];
        continue;
      }
    }
    if (solrHome == null || !new File(solrHome).exists()) {
      throw new IOException("You must specify a valid solr.home directory!");
    }
    TableMapReduceUtil.initTableMapperJob(inputTable, IndexingUtils.buildOccurrenceScan(),
      OccurrenceTableMapper.class, OccurrenceWritable.class,
      IntWritable.class, job);

    job.setReducerClass(SolrReducer.class);
    job.setOutputFormatClass(SolrOutputFormat.class);
    SolrOutputFormat.setupSolrHomeCache(new File(solrHome), job.getConfiguration());
    if (shards > 0) {
      job.setNumReduceTasks(shards);
    }
    job.setOutputKeyClass(OccurrenceWritable.class);
    job.setOutputValueClass(IntWritable.class);

    SolrDocumentConverter.setSolrDocumentConverter(OccurrenceObjectConverter.class, job.getConfiguration());

    FileOutputFormat.setOutputPath(job, out);

    SolrOutputFormat.setOutputZipFormat(compressOutput, job.getConfiguration());


    return job.waitForCompletion(true) ? 0 : -1;
  }
}
