package org.gbif.occurrence.cli.registry.sync;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncCommon {

  private static final Logger LOG = LoggerFactory.getLogger(SyncCommon.class);

  public static final String PROPS_FILE = "registry-sync.properties";

  public static Properties loadProperties() {
    Properties props = new Properties();
    try (InputStream in = SyncCommon.class.getClassLoader().getResourceAsStream(PROPS_FILE)) {
      props.load(in);
    } catch (Exception e) {
      LOG.error("Unable to open registry-sync.properties file - RegistrySync is not initialized", e);
    }

    return props;
  }
}
