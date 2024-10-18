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
package org.gbif.occurrence.ws.config;

import io.swagger.v3.core.converter.AnnotatedType;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.tags.Tag;
import org.gbif.api.vocabulary.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdoc.core.customizers.OpenApiCustomiser;
import org.springdoc.core.customizers.PropertyCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java configuration of the OpenAPI specification.
 */
@Component
public class OpenAPIConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(OpenAPIConfiguration.class);

  /**
   * Sorts tags (sections of the occurrence documentation) by the order extension, rather than alphabetically.
   */
  @Bean
  public OpenApiCustomiser sortTagsByOrderExtension() {
    return openApi -> {
      // Sort tags (end up as main sections on the left) by custom Extension value.
      openApi.setTags(openApi.getTags()
        .stream()
        .sorted(tagOrder())
        .collect(Collectors.toList()));

      // Sort operations (path+method) by custom Extension value.
      Paths paths = openApi.getPaths().entrySet()
        .stream()
        .sorted(Comparator.comparing(entry -> getOperationTag(entry.getValue())))
        .peek(e -> LOG.info("{} ← {}", getOperationTag(e.getValue()), e.getKey()))
        .collect(Paths::new, (map, item) -> map.addPathItem(item.getKey(), item.getValue()), Paths::putAll);
      openApi.setPaths(paths);

      // Set the list of enumeration values for the verbatimExtensions parameter in a predicate download
      // request to those supported, remembering to use the RowType
      List<String> allowedVerbatimExtensionValues = Extension.availableExtensions().stream().map(Extension::getRowType).collect(Collectors.toList());
      Schema predicateDownloadRequest = openApi.getComponents().getSchemas().get("PredicateDownloadRequest");
      ArraySchema verbatimExtension = (ArraySchema)  predicateDownloadRequest.getProperties().get("verbatimExtensions");
      Schema verbatimExtensionString = verbatimExtension.getItems();
      verbatimExtensionString.setEnum(allowedVerbatimExtensionValues);
    };
  }

  Comparator<Tag> tagOrder() {
    return Comparator.comparing(tag ->
      tag.getExtensions() == null ?
        "__" + tag.getName() :
        ((Map)tag.getExtensions().get("x-Order")).get("Order").toString());
  }

  private String getOperationTag(PathItem pathItem) {
    return Stream.of(
        pathItem.getGet(),
        pathItem.getHead(),
        pathItem.getPost(),
        pathItem.getPut(),
        pathItem.getDelete(),
        pathItem.getOptions(),
        pathItem.getTrace(),
        pathItem.getPatch())
      .filter(Objects::nonNull)
      .map(op -> getOperationOrder(op))
      .findFirst()
      .orElse("");
  }

  /**
   * Order by the x-Order tag if it's present, otherwise the operation id.
   */
  private String getOperationOrder(Operation op) {
    if (op.getExtensions() != null && op.getExtensions().containsKey("x-Order")) {
      return ((Map)op.getExtensions().get("x-Order")).get("Order").toString() + "_" + op.getOperationId();
    }
    return op.getOperationId();
  }
}
