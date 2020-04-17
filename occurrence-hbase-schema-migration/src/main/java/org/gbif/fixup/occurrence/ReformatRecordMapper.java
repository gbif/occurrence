package org.gbif.fixup.occurrence;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.NavigableMap;


/**
 * Reformat the records into the new schema.
 */
public class ReformatRecordMapper extends TableMapper<ImmutableBytesWritable, Put> {

  @Override
  protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
    int modulo = context.getConfiguration().getInt("modulo", 100);
    String unsalted = String.valueOf(Bytes.toLong(key.get()));
    byte[] saltedKey = saltKey(unsalted, modulo);
    context.setStatus(Bytes.toString(saltedKey));

    // build the new object
    Put put = new Put(saltedKey);

    // If we want Occurrence objects then we can use OccurrenceBuilder from the occurrence-persistence project

    // Or for example copy a single cell
    put.addColumn(
      Bytes.toBytes("fragment"),
      Bytes.toBytes("record"),
      result.getValue(Bytes.toBytes("o"), Bytes.toBytes("fragment"))
    );

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
