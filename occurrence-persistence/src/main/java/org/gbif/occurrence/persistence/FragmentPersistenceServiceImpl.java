package org.gbif.occurrence.persistence;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentCreationResult;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.InternalTerm;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.hbase.RowUpdate;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;

import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
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
      if (createKey) {
        KeyLookupResult keyLookupResult = keyService.generateKey(uniqueIds);
        keyCreated = keyLookupResult.isCreated();
        fragment.setKey(keyLookupResult.getKey());
      }
      writeFields(occTable, fragment);
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
   * Writes all fields of Fragment to the HBase table. Any field in the fragment that is
   * empty will be deleted from the corresponding "column" in the HBase row.
   *
   * @param occTable the HBase table to write to
   * @param frag the fragment to persist
   *
   * @throws IOException if communicating with HBase fails
   */
  private void writeFields(HTableInterface occTable, Fragment frag) throws IOException {

    RowUpdate upd = new RowUpdate(frag.getKey());

    upd.setInterpretedField(GbifTerm.datasetKey, frag.getDatasetKey());
    upd.setInterpretedField(GbifTerm.unitQualifier, frag.getUnitQualifier());
    upd.setInterpretedField(GbifTerm.protocol, frag.getProtocol());
    upd.setInterpretedField(GbifTerm.lastCrawled, frag.getHarvestedDate());
    upd.setInterpretedField(InternalTerm.crawlId, frag.getCrawlId());
    upd.setInterpretedField(InternalTerm.fragment, frag.getData());
    upd.setInterpretedField(InternalTerm.fragmentHash, frag.getDataHash());
    upd.setInterpretedField(InternalTerm.xmlSchema, frag.getXmlSchema());
    upd.setInterpretedField(InternalTerm.fragmentCreated, frag.getCreated());

    upd.execute(occTable);
  }
}
