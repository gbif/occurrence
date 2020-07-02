package org.gbif.occurrence.test.extensions;

import org.gbif.api.vocabulary.EndpointType;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.test.servers.HBaseServer;
import org.gbif.pipelines.fragmenter.common.HbaseStore;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Data
@Builder
public class FragmentInitializer implements BeforeAllCallback {

  private final String fragmentTableTable;
  private final String testDataFile;

  private static final ObjectMapper MAPPER = JacksonJsonObjectMapperProvider.getObjectMapper();

  /**
   * Calculates the fragment salted key.
   * This function was taken from {@link org.gbif.occurrence.persistence.OccurrencePersistenceServiceImpl}.
   */
  private String getSaltedKey(long key, long fragmenterSalt) {
    long mod = key % fragmenterSalt;
    String saltedKey = mod + ":" + key;
    return mod >= 10 ? saltedKey : "0" + saltedKey;
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    //Get required Spring bean
    ApplicationContext applicationContext = SpringExtension.getApplicationContext(extensionContext);
    HBaseServer hBaseServer = applicationContext.getBean(HBaseServer.class);
    OccHBaseConfiguration occHBaseConfiguration = applicationContext.getBean(OccHBaseConfiguration.class);

    TableName fragmentTableName = TableName.valueOf(fragmentTableTable);

    //Create fragment table
    hBaseServer.getHBaseTestingUtility().createTable(fragmentTableName, HbaseStore.getFragmentFamily());

    //Load fragment table using the JSON test data.
    try (Table fragmentTable = hBaseServer.getConnection().getTable(fragmentTableName);
         InputStream testDataFileStream = applicationContext.getResource(testDataFile).getInputStream()) {

      MAPPER.readTree(testDataFileStream).forEach(jsonNode -> {
        try {
          Map<String, String> record = Collections.singletonMap(getSaltedKey(jsonNode.get("gbifId").asLong(),
                                                                             occHBaseConfiguration.getFragmenterSalt()),
                                                                MAPPER.writeValueAsString(jsonNode));
          HbaseStore.putRecords(fragmentTable,
                                jsonNode.get("datasetKey").asText(),
                                jsonNode.get("crawlId").asInt(),
                                EndpointType.fromString(jsonNode.get("protocol").asText()),
                                record);
        } catch (JsonProcessingException ex) {
          throw new RuntimeException(ex);
        }
      });
    }
  }
}
