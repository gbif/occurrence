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
package org.gbif.occurrence.processor.interpreting.util;


import org.gbif.api.jackson.LicenseSerde;
import org.gbif.api.vocabulary.License;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Singleton;
/**
 * Provider that initializes the {@link ObjectMapper} to not fail on unknown properties.
 * This class also exists in common-ws, but this one here is compatible with jackson 1.8 and 1.9 so we can use it
 * also in Hive UDFs (which see jackson 1.8).
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ObjectMapperContextResolver implements ContextResolver<ObjectMapper> {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // determines whether encountering of unknown properties (ones that do not map to a property, and there is no
    // "any setter" or handler that can handle it) should result in a failure (throwing a JsonMappingException) or not.
    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // Enforce use of ISO-8601 format dates (http://wiki.fasterxml.com/JacksonFAQDateHandling)
    MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    // register predefined Mixins
    MAPPER.addMixIn(License.class, LicenseSerde.LicenseJsonSerializer.class);
  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return MAPPER;
  }
}
