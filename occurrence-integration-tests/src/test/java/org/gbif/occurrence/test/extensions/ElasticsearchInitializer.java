package org.gbif.occurrence.test.extensions;

import org.gbif.occurrence.test.servers.EsManageServer;

import lombok.Builder;
import lombok.Data;
import org.elasticsearch.action.bulk.BulkResponse;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Jupiter/JUnit5 extension class used to load test data into an Embedded Elasticssearch instance.
 */
@Data
@Builder
public class ElasticsearchInitializer implements BeforeAllCallback {

  private final String testDataFile;

  private EsManageServer esServer;

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    //Get the ES managed instance
    ApplicationContext applicationContext = SpringExtension.getApplicationContext(extensionContext);
    esServer = applicationContext.getBean(EsManageServer.class);

    //Index the test data
    BulkResponse bulkResponse = esServer.index(applicationContext.getResource(testDataFile));
    if (bulkResponse.hasFailures()) {
      throw new RuntimeException(bulkResponse.buildFailureMessage());
    }

    //refresh indices data
    esServer.refresh();
  }

}
