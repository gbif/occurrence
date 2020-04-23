package org.gbif.fixup.occurrence;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Reformat the records into the new schema.
 */
public class ReformatRecordMapper extends TableMapper<ImmutableBytesWritable, Put> {

  private static final byte[] FCF = Bytes.toBytes("fragment");
  private static final byte[] OCF = Bytes.toBytes("o");

  private static final byte[] DQ = Bytes.toBytes("datasetKey");
  private static final byte[] PQ = Bytes.toBytes("protocol");
  private static final byte[] RQ = Bytes.toBytes("record");
  private static final byte[] AQ = Bytes.toBytes("attempt");
  private static final byte[] CQ = Bytes.toBytes("crawlId");
  private static final byte[] DCQ = Bytes.toBytes("dateCreated");
  private static final byte[] DUQ = Bytes.toBytes("dateUpdated");

  @Override
  protected void map(ImmutableBytesWritable key, Result result, Context context)
    throws IOException, InterruptedException {
    String saltedKey = saltKey(Bytes.toLong(key.get()));
    context.setStatus(saltedKey);

    // build the new object
    Put put = new Put(saltedKey.getBytes());

    Cell datasetKeyCell = result.getColumnLatestCell(OCF, DQ);

    put.addColumn(FCF, DQ, result.getValue(OCF, DQ));
    put.addColumn(FCF, AQ, result.getValue(OCF, CQ));
    put.addColumn(FCF, PQ, result.getValue(OCF, PQ));
    put.addColumn(FCF, RQ, result.getValue(OCF, FCF));
    put.addColumn(FCF, DCQ, Bytes.toBytes(datasetKeyCell.getTimestamp()));
    put.addColumn(FCF, DUQ, Bytes.toBytes(datasetKeyCell.getTimestamp()));

    context.write(new ImmutableBytesWritable(saltedKey.getBytes()), put);
  }

  /*
   * Returns the unsalted key using a modulus based approach.
   * @param unsalted Key to salt
   * @return The salted key from 00:long to 99:long
   */
  public static String saltKey(Long key) {
    long salt = key % 100;
    String result = salt + ":" + key;
    return salt >= 10 ? result : "0" + result;
  }
}
