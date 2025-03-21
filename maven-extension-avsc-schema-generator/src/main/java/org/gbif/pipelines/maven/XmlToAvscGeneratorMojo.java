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
package org.gbif.pipelines.maven;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.digester.ThesaurusHandlingRule;
import org.gbif.dwc.extensions.ExtensionFactory;
import org.gbif.dwc.extensions.ExtensionProperty;
import org.gbif.dwc.extensions.VocabulariesManager;
import org.gbif.dwc.extensions.Vocabulary;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.xml.SAXUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import static java.nio.charset.StandardCharsets.UTF_8;

@Mojo(name = "avroschemageneration", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class XmlToAvscGeneratorMojo extends AbstractMojo {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  @Parameter(property = "avroschemageneration.pathToWrite")
  private String pathToWrite;

  @Parameter(property = "avroschemageneration.namespace")
  private String namespace;

  @Override
  public void execute() throws MojoExecutionException {

    try {
      Files.createDirectories(Paths.get(pathToWrite));

      Map<Extension, String> extensions = Extension.availableExtensionResources();

      for (Entry<Extension, String> extension : extensions.entrySet()) {

        try {
          String name = normalizeClassName(extension.getKey().name());
          URL url = new URL(extension.getValue());

          Path path = Paths.get(pathToWrite, normalizeFileName(name));
          System.out.println("Ext " + path);
          if (!Files.exists(path)) {
            convertAndWrite(name, url, path);
          }
        } catch (Exception ex) {
          getLog().warn(ex.getMessage());
        }
      }
    } catch (IOException ex) {
      throw new MojoExecutionException(ex.getMessage());
    }
  }

  public void setPathToWrite(String pathToWrite) {
    this.pathToWrite = pathToWrite;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  private void convertAndWrite(String name, URL url, Path path) throws Exception {
    // Read extension
    ThesaurusHandlingRule thr = new ThesaurusHandlingRule(new EmptyVocabulariesManager());
    ExtensionFactory factory = new ExtensionFactory(thr, SAXUtils.getNsAwareSaxParserFactory());
    org.gbif.dwc.extensions.Extension ext = factory.build(url.openStream(), url, false);

    // Convert into an avro schema
    List<Schema.Field> fields = new ArrayList<>(ext.getProperties().size() + 1);

    Map<String, Integer> duplicatesMap = new HashMap<>();
    ext.getProperties()
        .forEach(
            x -> {
              if (duplicatesMap.containsKey(x.getName())) {
                duplicatesMap.computeIfPresent(x.getName(), (s, i) -> ++i);
              } else {
                duplicatesMap.put(x.getName(), 1);
              }
            });

    // Add gbifID and datasetKey fields
    fields.add(createField("gbifid", Type.STRING, "GBIF internal identifier", false));
    fields.add(createField("datasetkey", Type.STRING, "GBIF registry dataset identifier", false));

    // Add terms from xml schema
    for (ExtensionProperty ep : ext.getProperties()) {

      String extFieldName = ep.getName();
      if (duplicatesMap.get(extFieldName) > 1) {
        extFieldName = TERM_FACTORY.findTerm(ep.qualifiedName()).prefixedName();
      }

      String fName = normalizeFieldName(extFieldName);
      // Add fields
      fields.add(createField(fName, ep.getQualname()));
      // Add RAW fields
      fields.add(createField(("v_" + fName).replaceAll("__+", "_"), ep.getQualname()));
    }

    String[] extraNamespace =
        url.toString().replaceAll("http://rs.gbif.org/extension/", "").split("/");

    String doc = "Avro Schema of Hive Table for " + name;
    String fullNamespace = namespace + "." + extraNamespace[0];
    String schema = Schema.createRecord(name, doc, fullNamespace, false, fields).toString(true);

    // Add comment
    String comment =
        "/** Autogenerated by maven-plugin-extension-xml-to-avsc. DO NOT EDIT DIRECTLY */\n";
    schema = comment + schema;

    // Save into a file
    Files.deleteIfExists(path);
    getLog().info("Create avro schema for " + ext.getName() + " extension - " + path);
    Files.write(path, schema.getBytes(UTF_8));
  }

  private Schema.Field createField(String name, String doc) {
    return createField(name, Type.STRING, doc, true);
  }

  private String normalizeClassName(String name) {
    return Arrays.stream(name.split("_"))
            .map(String::toLowerCase)
            .map(x -> x.substring(0, 1).toUpperCase() + x.substring(1))
            .collect(Collectors.joining())
        + "Table";
  }

  private String normalizeFieldName(String name) {
    String normalizedNamed = name.toLowerCase().trim()
      .replace("-", "")
      .replace("_", "")
      .replace(":", "_");
    if (Character.isDigit(normalizedNamed.charAt(0))) {
      return '_' + normalizedNamed;
    }
    return normalizedNamed;
  }

  private String normalizeFileName(String name) {
    String result =
        Arrays.stream(name.split("(?=[A-Z])"))
            .map(String::toLowerCase)
            .collect(Collectors.joining("-"));
    return result + ".avsc";
  }

  private Schema.Field createField(String name, Schema.Type type, String doc, boolean isNull) {

    Schema schema;
    if (isNull) {
      List<Schema> optionalString = new ArrayList<>(2);
      optionalString.add(Schema.create(Schema.Type.NULL));
      optionalString.add(Schema.create(type));
      schema = Schema.createUnion(optionalString);
      return new Schema.Field(name, schema, doc, JsonProperties.NULL_VALUE);
    } else {
      schema = Schema.create(type);
      return new Schema.Field(name, schema, doc);
    }

  }

  private static class EmptyVocabulariesManager implements VocabulariesManager {

    @Override
    public Vocabulary get(String uri) {
      return null;
    }

    @Override
    public Vocabulary get(URL url) {
      return null;
    }

    @Override
    public Map<String, String> getI18nVocab(String uri, String lang) {
      return null;
    }

    @Override
    public List<Vocabulary> list() {
      return null;
    }
  }
}
