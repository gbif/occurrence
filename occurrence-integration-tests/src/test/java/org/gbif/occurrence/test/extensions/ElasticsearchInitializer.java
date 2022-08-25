/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.test.extensions;

import org.gbif.occurrence.test.servers.EsManageServer;

import org.elasticsearch.action.bulk.BulkResponse;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import lombok.Builder;
import lombok.Data;

/**
 * Jupiter/JUnit5 extension class used to load test data into an Embedded Elasticssearch instance.
 */
@Data
@Builder
public class ElasticsearchInitializer implements BeforeAllCallback {

  private final String testDataFile;

  private EsManageServer esServer;

  @Override
  public void beforeAll(ExtensionContext extensionContext){
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
