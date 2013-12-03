/**
 * 
 */
package org.gbif.occurrence.index.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Reducer class that writes to a Solr Index.
 */
public class SolrReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
   */
  @Override
  protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
    super.setup(context);
    SolrRecordWriter.addReducerContext(context);
  }

}
