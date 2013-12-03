package org.gbif.occurrence.index.hbase;

import org.gbif.occurrence.index.solr.OccurrenceIndexDocument;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class OccurrenceWritable extends OccurrenceIndexDocument implements WritableComparable<OccurrenceWritable> {

  private static final int NULL_INT = -1;
  private static final String NULL_STRING = "NULL";
  private static final double NULL_DOUBLE = -999; // invalid for coordinates


  @Override
  public int compareTo(OccurrenceWritable obj) {
    if (obj.getKey() == null && getKey() == null) {
      return 0;
    } else if (obj.getKey() == null && getKey() != null) {
      return 1;
    } else if (obj.getKey() != null && getKey() == null) {
      return -1;
    } else {
      return obj.getKey().compareTo(getKey());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    setKey(readInt(in));
    setLatitude(readDouble(in));
    setLongitude(readDouble(in));
    setYear(readInt(in));
    setMonth(readInt(in));
    setCatalogNumber(readString(in));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    write(out, getKey());
    write(out, getLatitude());
    write(out, getLongitude());
    write(out, getYear());
    write(out, getMonth());
    write(out, getCatalogNumber());
  }

  private Double readDouble(DataInput in) throws IOException {
    double v = in.readDouble();
    return (v == NULL_DOUBLE) ? null : v;
  }

  private Integer readInt(DataInput in) throws IOException {
    int v = in.readInt();
    return (v == NULL_INT) ? null : v;
  }

  private String readString(DataInput in) throws IOException {
    String v = in.readUTF();
    return (NULL_STRING.equals(v)) ? null : v;
  }


  private void write(DataOutput out, Double d) throws IOException {
    double v = (d == null) ? NULL_DOUBLE : d;
    out.writeDouble(v);
  }

  private void write(DataOutput out, Integer i) throws IOException {
    int v = (i == null) ? NULL_INT : i;
    out.writeInt(v);
  }

  private void write(DataOutput out, String s) throws IOException {
    String v = (s == null || s.length() == 0) ? NULL_STRING : s;
    out.writeUTF(v);
  }

}
