package org.gbif.occurrence.persistence;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.persistence.experimental.OccurrenceRelationshipService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An implementation of OccurrenceService for retrieving Occurrence objects in HBase.
 */
@Component
public class OccurrencePersistenceServiceImpl implements OccurrenceService, OccurrenceRelationshipService {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrencePersistenceServiceImpl.class);

  private final String fragmenterTableName;
  private final int fragmenterSalt;
  private final String relationshipTableName;
  private final int relationshipSalt;
  private final Connection connection;

  @Autowired
  public OccurrencePersistenceServiceImpl(OccHBaseConfiguration cfg, Connection connection) {
    this.fragmenterTableName = checkNotNull(cfg.fragmenterTable, "fragmenterTable can't be null");
    this.fragmenterSalt = cfg.fragmenterSalt;
    this.relationshipTableName = cfg.relationshipTable;
    this.relationshipSalt = cfg.relationshipSalt;
    this.connection = checkNotNull(connection, "connection can't be null");
  }

  /**
   * Note that the returned fragment here is a String that holds the actual xml or json snippet for this occurrence,
   * and not the Fragment object that is used elsewhere.
   *
   * @param key that identifies an occurrence
   * @return a String holding the original xml or json snippet for this occurrence
   */
  @Override
  public String getFragment(long key) {
    String fragment = null;
    try (Table table = connection.getTable(TableName.valueOf(fragmenterTableName))) {

      String saltedKey = getSaltedKey(key);

      Get get = new Get(Bytes.toBytes(saltedKey));
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        LOG.info("Couldn't find occurrence for id [{}], returning null", key);
        return null;
      }
      byte[] rawFragment = result.getValue(Bytes.toBytes("fragment"), Bytes.toBytes("record"));
      if (rawFragment != null) {
        fragment = Bytes.toString(rawFragment);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
    }
    return fragment;
  }

  @Nullable
  @Override
  public VerbatimOccurrence getVerbatim(@Nullable Long key) {
    throw new UnsupportedOperationException("Replaced by pipelines");
  }

  @Override
  public Occurrence get(@Nullable Long key) {
    throw new UnsupportedOperationException("Replaced by pipelines");
  }

  @Nullable
  @Override
  public Occurrence get(UUID uuid, String s) {
    throw new UnsupportedOperationException("Implemented by OccurrenceGetByKey service");
  }

  @Override
  public List<String> getRelatedOccurrences(long key) {
    List<String> result = Lists.newArrayList();
    if (this.relationshipTableName != null) {
      try (Table table = connection.getTable(TableName.valueOf(relationshipTableName))) {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("o"));
        int salt = Math.abs(String.valueOf(key).hashCode()) % relationshipSalt;
        scan.setRowPrefixFilter(Bytes.toBytes(salt + ":" + key));
        ResultScanner s = table.getScanner(scan);

        Result row = s.next();
        int count=0;
        while (row != null && count++<100) {
          String reasons = Bytes.toString(row.getValue(Bytes.toBytes("o"), Bytes.toBytes("reasons")));
          // convert a -> ["a"] or a,b,c -> ["a","b", "c"]
          String reasonsAsJsonArray = "[\"" + reasons.replaceAll(",", "\",\"") + "\"]";

          String occurrence = Bytes.toString(row.getValue(Bytes.toBytes("o"), Bytes.toBytes("occurrence2")));
          result.add(String.format("{\n  \"reasons\": %s,\n  \"occurrence\":%s\n}", reasonsAsJsonArray, occurrence));
          row = s.next();
        }

      } catch (IOException e) {
        LOG.error("Could not read from HBase", e);
        throw new ServiceUnavailableException("Could not read from HBase [" + e.getMessage()+ "]");
      }
    }
    return result;
  }

  @Override
  public String getCurrentOccurrence(long key) {
    if (this.relationshipTableName != null) {
      try (Table table = connection.getTable(TableName.valueOf(relationshipTableName))) {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("o"));
        int salt = Math.abs(String.valueOf(key).hashCode()) % relationshipSalt;
        scan.setRowPrefixFilter(Bytes.toBytes(salt + ":" + key));
        ResultScanner s = table.getScanner(scan);
        Result row = s.next();
        if (row != null) {
          return Bytes.toString(row.getValue(Bytes.toBytes("o"), Bytes.toBytes("occurrence1")));
        }

      } catch (IOException e) {
        LOG.error("Could not read from HBase", e);
        throw new ServiceUnavailableException("Could not read from HBase [" + e.getMessage()+ "]");
      }
    }
    return "{}";
  }


    private String getSaltedKey(long key) {
    long mod = key % fragmenterSalt;
    String saltedKey = mod + ":" + key;
    return mod >= 10 ? saltedKey : "0" + saltedKey;
  }
}
