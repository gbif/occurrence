package org.gbif.occurrence.index.hbase;

import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.persistence.OccurrenceResultReader;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.index.hbase.IndexingUtils.buildOccurrenceScan;


/**
 * Class that creates a csv file using an occurrence hbase table.
 */
public class CSVFileBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(CSVFileBuilder.class);

  private static final String KEY_SOURCE_TABLE = "occurrence-index.sourceTable";
  private static final String[] STR_ARR_TYPE = new String[0];
  private final Properties props;


  /**
   * Default constructor.
   *
   * @param props configuration properties
   */
  public CSVFileBuilder(Properties props) {
    this.props = props;
  }

  /**
   * Utility to build an API Occurrence from an HBase row.
   *
   * @return A complete occurrence, or null
   */
  public static List<String> buildOccurrenceLine(@Nullable Result row) {
    List<String> result = Lists.newArrayList();
    if (row == null || row.isEmpty()) {
      return null;
    } else {
      result.add(objectToString(Integer.toString(Bytes.toInt(row.getRow()))));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_ELEVATION)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_BASIS_OF_RECORD)));
      result.add(objectToString(OccurrenceResultReader.getString(row, FieldName.CATALOG_NUMBER)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_CLASS_KEY)));
      result.add(objectToString(OccurrenceResultReader.getString(row, FieldName.I_CLASS)));
      result.add(objectToString(OccurrenceResultReader.getString(row, FieldName.COLLECTION_CODE)));
      result.add(objectToString(OccurrenceResultReader.getUuid(row, FieldName.DATASET_KEY)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_DEPTH)));
      result.add(objectToString(OccurrenceResultReader.getString(row, FieldName.I_FAMILY)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_FAMILY_KEY)));
      result.add(objectToString(OccurrenceResultReader.getString(row, FieldName.I_GENUS)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_GENUS_KEY)));
      result.add(objectToString(OccurrenceResultReader.getString(row, FieldName.INSTITUTION_CODE)));
      result.add(OccurrenceResultReader.getString(row, FieldName.I_COUNTRY));
      result.add(objectToString(OccurrenceResultReader.getString(row, FieldName.I_KINGDOM)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_KINGDOM_KEY)));
      result.add(objectToString(OccurrenceResultReader.getDouble(row, FieldName.I_DECIMAL_LATITUDE)));
      result.add(objectToString(OccurrenceResultReader.getDouble(row, FieldName.I_DECIMAL_LONGITUDE)));
      result.add(objectToString(OccurrenceResultReader.getDate(row, FieldName.I_MODIFIED)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_MONTH)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_TAXON_KEY)));
      result.add(objectToString(OccurrenceResultReader.getDate(row, FieldName.I_EVENT_DATE)));
      result.add(objectToString(OccurrenceResultReader.getString(row, FieldName.I_ORDER)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_ORDER_KEY)));
      result.add(objectToString(OccurrenceResultReader.getUuid(row, FieldName.PUB_ORG_KEY)));
      result.add(objectToString(OccurrenceResultReader.getString(row, FieldName.I_PHYLUM)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_PHYLUM_KEY)));
      result.add(objectToString(OccurrenceResultReader.getString(row, FieldName.I_SCIENTIFIC_NAME)));
      result.add(objectToString(OccurrenceResultReader.getString(row, FieldName.I_SPECIES)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_SPECIES_KEY)));
      result.add(objectToString(OccurrenceResultReader.getInteger(row, FieldName.I_YEAR)));

      return result;
    }
  }

  /**
   * Main method.
   * Requires 3 parameters: <hbaseInputTable> <solrHomeDir> <solrDataDir>.
   * hbaseInputTable: name of the hbase table.
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err
        .println("Usage: SequentialOccurrenceIndexBuilder <hbaseInputTable>");
      return;
    }
    Properties p = new Properties();
    p.setProperty(KEY_SOURCE_TABLE, args[0]);
    CSVFileBuilder builder = new CSVFileBuilder(p);
    builder.build();
  }


  private static String objectToString(Object o) {
    return o == null ? null : o.toString();
  }

  /**
   * Builds the Occurrence Solr index.
   */
  private void build() throws IOException {
    HTable table = null;
    ResultScanner scanner = null;
    CSVWriter writer = null;
    int numDocs = 0;

    // The doc instance is re-used

    LOG.info("Initiating indexing job");
    try {
      Configuration conf = HBaseConfiguration.create();
      table = new HTable(conf, props.getProperty(KEY_SOURCE_TABLE));
      Scan scan = buildOccurrenceScan();
      scan.setCaching(200);
      scanner = table.getScanner(scan);
      Iterator<Result> iter = scanner.iterator();
      writer = new CSVWriter(new FileWriter("occurrences.csv"));
      while (iter.hasNext() && numDocs < 10000) {
        writer.writeNext(buildOccurrenceLine(iter.next()).toArray(STR_ARR_TYPE));
        numDocs++;
      }
    } catch (Exception ex) {
      LOG.error("A general error has occurred", ex);
    } finally {
      Closeables.closeQuietly(scanner);
      Closeables.closeQuietly(table);
      Closeables.closeQuietly(writer);
      LOG.info("Indexing job has finished, # of documents indexed: {}", numDocs);
    }
  }
}
