package org.gbif.occurrence.persistence;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentCreationResult;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.constants.HBaseTableConstants;
import org.gbif.occurrence.persistence.hbase.HBaseHelper;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;

import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reads and writes Fragments to/from HBase.
 */
@Singleton
public class FragmentPersistenceServiceImpl implements FragmentPersistenceService {

  private static final Logger LOG = LoggerFactory.getLogger(FragmentPersistenceServiceImpl.class);

  private final String occurrenceTableName;
  private final HTablePool tablePool;
  private final OccurrenceKeyPersistenceService keyService;

  @Inject
  public FragmentPersistenceServiceImpl(@Named("table_name") String tableName, HTablePool tablePool,
    OccurrenceKeyPersistenceService keyService) {
    this.occurrenceTableName = tableName;
    this.tablePool = tablePool;
    this.keyService = keyService;
  }

  /**
   * Simple HBase get for given key. Note that OccurrenceBuilder could throw ValidationException if the underlying
   * fragment is missing required fields.
   *
   * @param key the key of the fragment (Integer rather than int for use in methods/classes using generic types)
   *
   * @return the fragment or null if not found
   */
  @Override
  public Fragment get(@Nullable Integer key) {
    if (key == null) {
      return null;
    }

    Fragment frag = null;
    HTableInterface table = null;
    try {
      table = tablePool.getTable(occurrenceTableName);
      Get get = new Get(Bytes.toBytes(key));
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        LOG.debug("Couldn't find fragment for occurrence key [{}], returning null", key);
      } else {
        frag = OccurrenceBuilder.buildFragment(result);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
          LOG.warn("Couldn't return table to pool - continuing with possible memory leak", e);
        }
      }
    }

    return frag;
  }

  @Override
  public FragmentCreationResult insert(Fragment fragment, Set<UniqueIdentifier> uniqueIds) {
    checkNotNull(fragment, "fragment can't be null");
    checkNotNull(uniqueIds, "uniqueIds can't be null");

    fragment.setCreated(System.currentTimeMillis());
    boolean keyCreated = write(fragment, true, uniqueIds);

    return new FragmentCreationResult(fragment, keyCreated);
  }

  @Override
  public void update(Fragment fragment) {
    checkNotNull(fragment, "fragment can't be null");
    checkNotNull(fragment.getKey(), "fragment's key can't be null for update");

    write(fragment, false, null);
  }


  /**
   * Returns true if a key was created, false otherwise.
   */
  private boolean write(Fragment fragment, boolean createKey, @Nullable Set<UniqueIdentifier> uniqueIds) {
    if (createKey) {
      checkNotNull(uniqueIds, "uniqueIds can't be null if createKey is true");
    }

    boolean keyCreated = false;
    HTableInterface occTable = null;
    try {
      occTable = tablePool.getTable(occurrenceTableName);
      byte[] cf = Bytes.toBytes(HBaseTableConstants.OCCURRENCE_COLUMN_FAMILY);
      if (createKey) {
        KeyLookupResult keyLookupResult = keyService.generateKey(uniqueIds);
        keyCreated = keyLookupResult.isCreated();
        fragment.setKey(keyLookupResult.getKey());
      }
      writeFields(occTable, cf, fragment, true);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    } finally {
      if (occTable != null) {
        try {
          occTable.close();
        } catch (IOException e) {
          LOG.warn("Couldn't return table to pool - continuing with possible memory leak", e);
        }
      }
    }

    return keyCreated;
  }

  /**
   * Writes all fields of Fragment to the HBase table. If dn (deleteNulls) is true, any field in the fragment that is
   * empty will be deleted from the corresponding "column" in the HBase row.  If dn is false, any data already existing
   * in the HBase table for empty fields in the Fragment will remain.
   *
   * @param occTable the HBase table to write to
   * @param cf the column family of the table to write to
   * @param frag the fragment to persist
   * @param dn deleteNulls?
   *
   * @throws IOException if communicating with HBase fails
   */
  private static void writeFields(HTableInterface occTable, byte[] cf, Fragment frag, boolean dn) throws IOException {
    Put put = new Put(Bytes.toBytes(frag.getKey()));
    Delete del = null;
    if (dn) del = new Delete(Bytes.toBytes(frag.getKey()));

    HBaseHelper.writeField(FieldName.DATASET_KEY,
      frag.getDatasetKey() == null ? null : Bytes.toBytes(frag.getDatasetKey().toString()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.CRAWL_ID,
      frag.getCrawlId() == null ? null : Bytes.toBytes(frag.getCrawlId()), dn, cf, put, del);
    HBaseHelper.writeField(GbifTerm.unitQualifier,
      frag.getUnitQualifier() == null ? null : Bytes.toBytes(frag.getUnitQualifier()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.HARVESTED_DATE,
      frag.getHarvestedDate() == null ? null : Bytes.toBytes(frag.getHarvestedDate().getTime()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.FRAGMENT, frag.getData(), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.FRAGMENT_HASH, frag.getDataHash(), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.XML_SCHEMA,
      frag.getXmlSchema() == null ? null : Bytes.toBytes(frag.getXmlSchema().toString()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.PROTOCOL,
      frag.getProtocol() == null ? null : Bytes.toBytes(frag.getProtocol().toString()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.CREATED,
      frag.getCreated() == null ? null : Bytes.toBytes(frag.getCreated()), dn, cf, put, del);

    occTable.put(put);
    if (dn && !del.isEmpty()) occTable.delete(del);
    occTable.flushCommits();
  }
}
