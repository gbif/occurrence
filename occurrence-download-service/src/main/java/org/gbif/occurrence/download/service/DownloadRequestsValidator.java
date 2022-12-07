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
package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.PredicateDownloadRequest;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class to validate PredicateDownloadRequest against a Json Schema.
 * This class is implemented base on schemas because Jackson doesn't recognise unknown properties when objects
 * are created using the JsonCreator annotation on a constructor.
 */
@Slf4j
public class DownloadRequestsValidator {

  private final Schema predicateDownloadRequestSchema;

  public DownloadRequestsValidator() {
    predicateDownloadRequestSchema = predicateDownloadRequestSchema();
  }

  /**
   * Creates an initializes the Json schema.
   */
  @SneakyThrows
  private static Schema predicateDownloadRequestSchema(){

    ObjectMapper mapper = new ObjectMapper();
    JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper, JsonSchemaConfig.nullableJsonSchemaDraft4());
    JsonNode schemaNode = schemaGen.generateJsonSchema(PredicateDownloadRequest.class);

    JSONObject rawSchema = new JSONObject(new JSONTokener(mapper.writeValueAsString(schemaNode)));

    JSONObject notificationAddresses = rawSchema.getJSONObject("properties").getJSONObject("notificationAddresses");
    //Adding 2 artificial fields to allow snake and camel case format in some properties
    rawSchema.getJSONObject("properties")
      .put("send_notification", rawSchema.getJSONObject("properties").getJSONObject("sendNotification"))
      .put("notification_addresses", notificationAddresses)
      .put("notificationAddress", notificationAddresses)
      .put("notification_address", notificationAddresses);

    log.info("Download requests will be validated against schema {}", rawSchema);
    return SchemaLoader.load(rawSchema);
  }


  /** Validates the predicate download request as Json against the schema.*/
  @SneakyThrows
  public void validate(String jsonPredicateDownloadRequest) {
    predicateDownloadRequestSchema.validate(new JSONObject(jsonPredicateDownloadRequest));
  }
}
