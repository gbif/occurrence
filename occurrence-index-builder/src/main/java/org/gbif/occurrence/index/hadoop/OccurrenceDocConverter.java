/**
 * 
 */
package org.gbif.occurrence.index.hadoop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Converts a occurrence record into a Solr document.
 */
public class OccurrenceDocConverter extends SolrDocumentConverter<LongWritable, Text> {

  public static final String[] FIELDS = {"key", "latitude", "longitude", "year", "month", "catalog_number"};
  private static final String NO_VALUE = "\\N";
  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceDocConverter.class);

  @Override
  public Collection<SolrInputDocument> convert(LongWritable key, Text value) {
    SolrInputDocument doc = new SolrInputDocument();
    ArrayList<SolrInputDocument> list = new ArrayList<SolrInputDocument>();

    String text = value.toString();
    StringTokenizer stringTokenizer = new StringTokenizer(text, "\001");
    for (int i = 0; stringTokenizer.hasMoreElements(); i++) {
      String fieldName = FIELDS[i];
      String fieldValue = stringTokenizer.nextToken();
      try {
        if (!fieldValue.equals(NO_VALUE)) {
          if (fieldName.equals("latitude") || fieldName.equals("longitude")) {
            doc.addField(fieldName, Double.parseDouble(fieldValue));
          } else if (fieldName.equals("catalog_number")) {
            doc.addField(fieldName, fieldValue);
          }
          else {
            doc.addField(fieldName, Integer.parseInt(fieldValue));
          }
        }
      } catch (Exception e) {
        LOG.error("Error converting field: " + fieldName + " value: " + fieldValue, e);
      }
    }
    list.add(doc);
    return list;
  }
}
