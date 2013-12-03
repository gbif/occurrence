package org.gbif.occurrence.index.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IdentityMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

}
