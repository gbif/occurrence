package org.gbif.occurrence.cli.common;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple file reader for CSV files that are the result of Hue queries.
 */
public class HueCsvReader {

  private static final Logger LOG = LoggerFactory.getLogger(HueCsvReader.class);
  private static final Set<String> HEADER_FIELDS = ImmutableSet.of("id", "dataset_id", "key", "dataset_key");

  /**
   * Read the CSV file export from a Hue query for keys (e.g. select key from occurrence where xxx or
   * select dataset_key from occurrence where xxx). Properly handles the header row "id" and double quotes that Hue
   * adds.
   *
   * @param keyFileName the file to read
   *
   * @return the keys as Strings
   */
  public static List<String> readKeys(String keyFileName) {
    List<String> keys = Lists.newArrayList();
    BufferedReader br = null;
    try {
        br = new BufferedReader(new FileReader(keyFileName));
        String line;
        while ((line = br.readLine()) != null) {
          String key = line.replaceAll("\"", "");
          if (!HEADER_FIELDS.contains(key)) {
            keys.add(key);
          }
        }
    } catch (FileNotFoundException e) {
      LOG.error("Could not find csv key file [{}]. Exiting", keyFileName, e);
    } catch (IOException e) {
      LOG.error("Error while reading csv key file [{}]. Exiting", keyFileName, e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          LOG.warn("Couldn't close key file. Attempting to continue anyway.", e);
        }
      }
    }

    return keys;
  }

  public static List<Integer> readIntKeys(String keyFileName) {
    List<Integer> keys = Lists.newArrayList();
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(keyFileName));
      String line;
      while ((line = br.readLine()) != null) {
        try {
          Integer key = Integer.valueOf(line.trim());
          keys.add(key);
        } catch (NumberFormatException e) {
          LOG.error("Ignore invalid integer key: {}", line);
        }
      }
    } catch (FileNotFoundException e) {
      LOG.error("Could not find csv key file [{}]. Exiting", keyFileName, e);
    } catch (IOException e) {
      LOG.error("Error while reading csv key file [{}]. Exiting", keyFileName, e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          LOG.warn("Couldn't close key file. Attempting to continue anyway.", e);
        }
      }
    }

    return keys;
  }
}
