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
  private static final byte[] RQ =  Bytes.toBytes("record");
  private static final byte[] AQ = Bytes.toBytes("attempt");
  private static final byte[] CQ = Bytes.toBytes("crawlId");
  private static final byte[] DCQ = Bytes.toBytes("dateCreated");
  private static final byte[] DUQ = Bytes.toBytes("dateUpdated");

  @Override
  protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
    int modulo = context.getConfiguration().getInt("modulo", 100);
    String unsalted = String.valueOf(Bytes.toLong(key.get()));
    byte[] saltedKey = saltKey(unsalted, modulo);
    context.setStatus(Bytes.toString(saltedKey));

    // build the new object
    Put put = new Put(saltedKey);

    Cell datasetKeyCell = result.getColumnLatestCell(OCF, DQ);

    put.addColumn(FCF, DQ, result.getValue(OCF, DQ));
    put.addColumn(FCF, AQ, result.getValue(OCF, CQ));
    put.addColumn(FCF, PQ, result.getValue(OCF, PQ));
    put.addColumn(FCF, RQ, result.getValue(OCF, FCF));
    put.addColumn(FCF, DCQ, Bytes.toBytes(datasetKeyCell.getTimestamp()));
    put.addColumn(FCF, DUQ, Bytes.toBytes(datasetKeyCell.getTimestamp()));

    context.write(new ImmutableBytesWritable(saltedKey), put);
  }
   /*
   * Returns the unsalted key using a modulus based approach.
   * @param unsalted Key to salt
   * @param numberOfBuckets To use in salting
   * @return The salted key
   */
  public static byte[] saltKey(String unsalted, int numberOfBuckets) {
    int salt = Math.abs(unsalted.hashCode() % numberOfBuckets);
    int digitCount = digitCount(numberOfBuckets-1);  // minus one because e.g. %100 produces 0..99 (2 digits)
    String saltedKey = leftPadZeros(salt,digitCount) + ":" + unsalted;
    return Bytes.toBytes(saltedKey);
  }

  /**
   * Pads with 0s to desired length.
   * @param number To pad
   * @param length The final length needed
   * @return The string padded with 0 if needed
   */
  static String leftPadZeros(int number, int length) {
    return String.format("%0" + length + "d", number);
  }

  /**
   * Returns the number of digits in the number.  This will only provide sensible results for number>0 and the input
   * is not sanitized.
   *
   * @return the number of digits in the number
   */
  private static int digitCount(int number) {
    return (int)(Math.log10(number)+1);
  }
}
