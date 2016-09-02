package org.gbif.occurrence.cli.registry.sync;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.occurrence.persistence.hbase.ExtResultReader;

import java.util.UUID;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The class is responsible to know the rules to determine if an Occurrence record should be updated or not after
 * a change coming from the Registry. It is also responsible to "mutate" an Occurrence object to its new state.
 */
public class RegistryBasedOccurrenceMutator {

  /**
   * Check if a HBase {@link Result} representing an Occurrence requires an update based on a dataset.
   * @param dataset
   * @param hbaseValues
   * @return
   */
  public boolean requiresUpdate(Dataset dataset, Organization publishingOrg, Result hbaseValues) {
    Preconditions.checkNotNull(dataset, "Dataset must be provided");
    UUID newPublishingOrgKey = dataset.getPublishingOrganizationKey();

    String rawPublishingOrgKey = Bytes.toString(hbaseValues.getValue(SyncCommon.OCC_CF, SyncCommon.OOK_COL));
    UUID publishingOrgKey = rawPublishingOrgKey == null ? null : UUID.fromString(rawPublishingOrgKey);

    String rawHostCountry = Bytes.toString(hbaseValues.getValue(SyncCommon.OCC_CF, SyncCommon.HC_COL));
    Country hostCountry = rawHostCountry == null ? null : Country.fromIsoCode(rawHostCountry);
    Country newHostCountry = publishingOrg.getCountry();

    License recordLicense = ExtResultReader.getEnum(hbaseValues, DcTerm.license, License.class);

    return !(newPublishingOrgKey.equals(publishingOrgKey) &&
            newHostCountry == hostCountry &&
            dataset.getLicense().equals(recordLicense));
  }

  /**
   * Mutate the provided {@link Occurrence} based on information from the Registry.
   * @param occurrence
   * @param dataset
   * @param publishingOrg
   */
  public void mutateOccurrence(Occurrence occurrence, Dataset dataset, Organization publishingOrg) {
    occurrence.setPublishingOrgKey(publishingOrg.getKey());
    occurrence.setPublishingCountry(publishingOrg.getCountry());
    occurrence.setLicense(dataset.getLicense());
  }
}
