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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

public class XmlToAvscGeneratorMojoTest {

  @Test
  public void mojoTest() throws Exception {

    // State
    String path = getClass().getResource("/generated/").getFile();

    XmlToAvscGeneratorMojo mojo = new XmlToAvscGeneratorMojo();
    mojo.setNamespace("org.gbif.pipelines.io.avro");
    mojo.setPathToWrite(path);

    // When
    mojo.execute();

    // Should
    Path result = Paths.get(path, "identification-table.avsc");
    Assert.assertTrue(Files.exists(result));

    Schema schema = new Schema.Parser().parse(result.toFile());
    Assert.assertEquals("IdentificationTable", schema.getName());
    Assert.assertEquals("org.gbif.pipelines.io.avro.dwc", schema.getNamespace());
    Assert.assertEquals(78, schema.getFields().size());
    Assert.assertNotNull(schema.getField("order_"));
  }
}
