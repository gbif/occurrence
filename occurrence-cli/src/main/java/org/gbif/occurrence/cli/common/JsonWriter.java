package org.gbif.occurrence.cli.common;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Simple writer to serialize {@link Object} as JSON into a file.
 * This class holds a shared {@link ObjectMapper} instance.
 */
public class JsonWriter {

  private static final ObjectMapper OM = new ObjectMapper();
  static {
    OM.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
  }

  /**
   * Serialize an {@link Object} to JSON into a file.
   * @param outputFilepath
   * @param obj
   * @throws IOException
   */
  public static void objectToJsonFile(String outputFilepath, Object obj) throws IOException {
    //creates folder structure (if required)
    File reportFile = new File(outputFilepath);
    if(!reportFile.getParentFile().exists()) {
      reportFile.getParentFile().mkdirs();
    }
    OM.writeValue(reportFile, obj);
  }

  /**
   * Serialize an {@link Object} to JSON into {@link String}.
   * @param obj
   * @return
   * @throws IOException
   */
  public static String objectToJsonString(Object obj) throws IOException {
    return OM.writeValueAsString(obj);
  }

}
