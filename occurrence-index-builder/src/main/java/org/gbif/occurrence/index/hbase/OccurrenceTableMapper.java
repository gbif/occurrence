package org.gbif.occurrence.index.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;


public class OccurrenceTableMapper extends TableMapper<OccurrenceWritable, IntWritable> {

  private final IntWritable ONE = new IntWritable(1);

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object,
   * org.apache.hadoop.mapreduce.Mapper.Context)
   */
  @Override
  protected void map(ImmutableBytesWritable key, Result value, org.apache.hadoop.mapreduce.Mapper.Context context)
    throws IOException, InterruptedException {
    context.write(IndexingUtils.buildOccurrenceWritableObject(value), ONE);
  }

}
