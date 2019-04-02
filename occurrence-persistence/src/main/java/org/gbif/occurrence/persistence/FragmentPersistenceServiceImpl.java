package org.gbif.occurrence.persistence;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.FragmentCreationResult;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.hbase.RowUpdate;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
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
  private final Connection connection;
  private final OccurrenceKeyPersistenceService keyService;

  @Inject
  public FragmentPersistenceServiceImpl(OccHBaseConfiguration cfg, Connection connection,
                                        OccurrenceKeyPersistenceService keyService) {
    occurrenceTableName = cfg.occTable;
    this.connection = connection;
    this.keyService = keyService;
  }

  /**
   * Simple HBase get for given key. Note that OccurrenceBuilder could throw ValidationException if the underlying
   * fragment is missing required fields.
   *
   * @param key the key of the fragment (Long rather than long for use in methods/classes using generic types)
   *
   * @return the fragment or null if not found
   */
  @Override
  public Fragment get(@Nullable Long key) {
    if (key == null) {
      return null;
    }

    Fragment frag = null;
    try (Table table = connection.getTable(TableName.valueOf(occurrenceTableName))) {
      Get get = new Get(Bytes.toBytes(key));
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        LOG.debug("Couldn't find fragment for occurrence key [{}], returning null", key);
      } else {
        frag = OccurrenceBuilder.buildFragment(result);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
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
    try (Table table = connection.getTable(TableName.valueOf(occurrenceTableName))) {
      if (createKey) {
        KeyLookupResult keyLookupResult = keyService.generateKey(uniqueIds);
        keyCreated = keyLookupResult.isCreated();
        fragment.setKey(keyLookupResult.getKey());
      }
      writeFields(table, fragment);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    }

    return keyCreated;
  }

  /**
   * Writes all fields of Fragment to the HBase table. Any field in the fragment that is
   * empty will be deleted from the corresponding "column" in the HBase row.
   *
   * @param occTable the HBase table to write to
   * @param frag     the fragment to persist
   *
   * @throws IOException if communicating with HBase fails
   */
  private void writeFields(Table occTable, Fragment frag) throws IOException {

    Fragment oldFrag = get(frag.getKey());
    boolean isInsert = oldFrag == null;
    RowUpdate upd = new RowUpdate(frag.getKey());

    if (isInsert || !Objects.equals(oldFrag.getDatasetKey(), frag.getDatasetKey())) {
      upd.setInterpretedField(GbifTerm.datasetKey, frag.getDatasetKey());
    }
    if (isInsert || !Objects.equals(oldFrag.getUnitQualifier(), frag.getUnitQualifier())) {
      upd.setInterpretedField(GbifInternalTerm.unitQualifier, frag.getUnitQualifier());
    }
    if (isInsert || !Objects.equals(oldFrag.getProtocol(), frag.getProtocol())) {
      upd.setInterpretedField(GbifTerm.protocol, frag.getProtocol());
    }
    if (isInsert || !Objects.equals(oldFrag.getHarvestedDate(), frag.getHarvestedDate())) {
      upd.setInterpretedField(GbifTerm.lastCrawled, frag.getHarvestedDate());
    }
    if (isInsert || !Objects.equals(oldFrag.getCrawlId(), frag.getCrawlId())) {
      upd.setInterpretedField(GbifInternalTerm.crawlId, frag.getCrawlId());
    }
    if (isInsert || !Arrays.equals(oldFrag.getData(), frag.getData())) {
      upd.setInterpretedField(GbifInternalTerm.fragment, frag.getData());
    }
    if (isInsert || !Arrays.equals(oldFrag.getDataHash(), frag.getDataHash())) {
      upd.setInterpretedField(GbifInternalTerm.fragmentHash, frag.getDataHash());
    }
    if (isInsert || !Objects.equals(oldFrag.getXmlSchema(), frag.getXmlSchema())) {
      upd.setInterpretedField(GbifInternalTerm.xmlSchema, frag.getXmlSchema());
    }
    if (isInsert || !Objects.equals(oldFrag.getCreated(), frag.getCreated())) {
      upd.setInterpretedField(GbifInternalTerm.fragmentCreated, frag.getCreated());
    }

    if (!upd.getRowMutations().getMutations().isEmpty()) {
      upd.execute(occTable);
    }
  }
}
