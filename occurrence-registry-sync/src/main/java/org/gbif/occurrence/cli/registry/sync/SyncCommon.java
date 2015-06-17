package org.gbif.occurrence.cli.registry.sync;

import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.persistence.hbase.Columns;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncCommon {

  private static final Logger LOG = LoggerFactory.getLogger(SyncCommon.class);
  private static final String PROPS_FILE = "registry-sync.properties";

  public static final String OCC_TABLE_PROPS_KEY = "occurrence.db.table_name";
  public static final String REG_WS_PROPS_KEY = "registry.ws.url";
  public static final byte[] OCC_CF = Columns.CF;
  public static final byte[] DK_COL = Bytes.toBytes(Columns.column(GbifTerm.datasetKey));
  public static final byte[] OOK_COL = Bytes.toBytes(Columns.column(GbifInternalTerm.publishingOrgKey));
  public static final byte[] HC_COL = Bytes.toBytes(Columns.column(GbifTerm.publishingCountry));
  public static final byte[] CI_COL = Bytes.toBytes(Columns.column(GbifInternalTerm.crawlId));

  public static Properties loadProperties() {
    Properties props = new Properties();
    InputStream in = null;
    try {
      in = SyncCommon.class.getClassLoader().getResourceAsStream(PROPS_FILE);
      props.load(in);
    } catch (Exception e) {
      LOG.error("Unable to open registry-sync.properties file - RegistrySync is not initialized", e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          LOG.info("Failed to close input stream for registry-sync.properties file - continuing anyway.", e);
        }
      }
    }

    return props;
  }
}
