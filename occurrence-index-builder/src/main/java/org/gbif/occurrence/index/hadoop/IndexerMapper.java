package org.gbif.occurrence.index.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IndexerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

  private SolrServerFactory solrServerFactory;
  private OccurrenceDocConverter occurrenceDocConverter;
  private static final Logger LOG = LoggerFactory.getLogger(IndexerMapper.class);

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
   */
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object,
   * org.apache.hadoop.mapreduce.Mapper.Context)
   */
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    while (context.nextKeyValue()) {
      try {
        solrServerFactory.getSolr().add(
          occurrenceDocConverter.convert(context.getCurrentKey(), context.getCurrentValue()));
      } catch (SolrServerException e) {
        LOG.error("Error adding document", e);
      }
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
   */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    solrServerFactory = new SolrServerFactory(context);
    occurrenceDocConverter = new OccurrenceDocConverter();
  }
}
