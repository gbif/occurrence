package org.gbif.occurrence.test.extensions;

import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.test.servers.HBaseServer;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Jupiter/Junit5 extension to initialize an HBase test table to support the experimental/related service.
 */
@Data
@Builder
public class OccurrenceRelationshipInitializer implements BeforeAllCallback {

  private final String relationshipTableName;
  private final String testDataFile;

  private static final ObjectMapper MAPPER = JacksonJsonObjectMapperProvider.getObjectMapper();

  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("o");

  private static final byte[] OCCURRENCE1_COLUMN = Bytes.toBytes("occurrence1");

  private static final byte[] OCCURRENCE2_COLUMN = Bytes.toBytes("occurrence2");

  private static final byte[] ID1_COLUMN = Bytes.toBytes("id1");

  private static final byte[] ID2_COLUMN = Bytes.toBytes("id2");

  private static final byte[] DATASE1_COLUMN = Bytes.toBytes("dataset1");

  private static final byte[] DATASE2_COLUMN = Bytes.toBytes("dataset2");

  private static final byte[] RELATIONSHIP_TYPE_COLUMN = Bytes.toBytes("relationshipType");

  /**
   * Calculates the fragment salted key.
   * This function was taken from {@link org.gbif.occurrence.persistence.OccurrencePersistenceServiceImpl}.
   */
  private String getSaltedKey(long key, long salt) {
    long mod = Math.abs(String.valueOf(key).hashCode()) % salt;
    return mod + ":" + key;
  }

  @SneakyThrows
  private static String toString(JsonNode jsonNode) {
    return MAPPER.writeValueAsString(jsonNode);
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    //Get required Spring bean
    ApplicationContext applicationContext = SpringExtension.getApplicationContext(extensionContext);
    HBaseServer hBaseServer = applicationContext.getBean(HBaseServer.class);
    OccHBaseConfiguration occHBaseConfiguration = applicationContext.getBean(OccHBaseConfiguration.class);

    TableName relationshipTableName = TableName.valueOf(occHBaseConfiguration.getRelationshipTable());

    //Create fragment table
    hBaseServer.getHBaseTestingUtility().createTable(relationshipTableName, COLUMN_FAMILY);

    //Load fragment table using the JSON test data.
    try (Table relationshipTable = hBaseServer.getConnection().getTable(relationshipTableName);
         InputStream testDataFileStream = applicationContext.getResource(testDataFile).getInputStream()) {
      List<Put> puts = new ArrayList<>();
      MAPPER.readTree(testDataFileStream).forEach(jsonNode -> {
        String id1 = jsonNode.get("occurrence").get("key").asText();
        byte[] datasetKey1 = Bytes.toBytes(jsonNode.get("occurrence").get("datasetKey").asText());
        String saltedKey = getSaltedKey(Long.parseLong(id1), occHBaseConfiguration.getRelationshipSalt());
        byte[] occurrence1 = Bytes.toBytes(toString(jsonNode.get("occurrence")));
        StreamSupport.stream(((ArrayNode)jsonNode.get("relationships")).spliterator(), false)
          .forEach(relation -> {
              String id2 = relation.get("occurrence").get("gbifId").asText();
              Put put = new Put(Bytes.toBytes(saltedKey + ":" + id2));
              put.addColumn(COLUMN_FAMILY, ID1_COLUMN, Bytes.toBytes(id1));
              put.addColumn(COLUMN_FAMILY, ID2_COLUMN, Bytes.toBytes(id2));
              put.addColumn(COLUMN_FAMILY, DATASE1_COLUMN, datasetKey1);
              put.addColumn(COLUMN_FAMILY, DATASE2_COLUMN, Bytes.toBytes(relation.get("occurrence").get("datasetKey").asText()));
              put.addColumn(COLUMN_FAMILY, OCCURRENCE1_COLUMN, occurrence1);
              put.addColumn(COLUMN_FAMILY, OCCURRENCE2_COLUMN, Bytes.toBytes(toString(relation.get("occurrence"))));
              put.addColumn(COLUMN_FAMILY, RELATIONSHIP_TYPE_COLUMN, Bytes.toBytes(relation.get("relationshipType").asText()));
              puts.add(put);
        });
      });
      relationshipTable.put(puts);
    }
  }
}
