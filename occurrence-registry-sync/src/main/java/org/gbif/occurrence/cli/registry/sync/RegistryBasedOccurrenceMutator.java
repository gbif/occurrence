package org.gbif.occurrence.cli.registry.sync;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.occurrence.persistence.hbase.ExtResultReader;

import java.text.MessageFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
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
   * Check if a HBase {@link Result} representing an Occurrence requires an update based on a {@link Dataset} and
   * an {@link Organization}.
   * Note to devs: make sure the value you extract from {@link Result} is available in the Scan used.
   *
   * @param dataset
   * @param hbaseValues
   *
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

    return !(Objects.equals(newPublishingOrgKey, publishingOrgKey) &&
            Objects.equals(newHostCountry, hostCountry) &&
            Objects.equals(dataset.getLicense(), recordLicense));
  }

  /**
   * Check if changes on a dataset should trigger an update of Occurrence records.
   *
   * @param currentDataset
   * @param newDataset
   *
   * @return
   */
  public boolean requiresUpdate(Dataset currentDataset, Dataset newDataset) {
    // A change in publishing organization requires an update
    if (!Objects.equals(currentDataset.getPublishingOrganizationKey(), newDataset.getPublishingOrganizationKey())) {
      return true;
    }

    // A change in license requires an update.
    if (currentDataset.getLicense() != null && !Objects.equals(currentDataset.getLicense(), newDataset.getLicense())) {

      // New datasets are created as CC_BY_4_0, and very quickly updated to the actual license. This is odd, see
      // https://github.com/gbif/registry/issues/71
      // Until that is resolved, we don't create m/r sync jobs for datasets that are under 3 minutes old.
      Instant threeMinutesAgo = Instant.now().minusSeconds(180);
      if (currentDataset.getCreated().toInstant().isAfter(threeMinutesAgo)) {
        return false;
      }

      return true;
    }

    return false;
  }

  /**
   * Check if changes on an organization should trigger an update of Occurrence records of all its datasets.
   *
   * @param currentOrg
   * @param newOrg
   * @return
   */
  public boolean requiresUpdate(Organization currentOrg, Organization newOrg) {
    // endorsement not approved means that we don't have records so nothing to update
    if (!newOrg.isEndorsementApproved()) {
      return false;
    }
    return !(Objects.equals(currentOrg.getCountry(), newOrg.getCountry()));
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

  /**
   * Generates a message about what changed in the mutation. Mostly use for logging.
   *
   * @param currentOrg
   * @param newOrg
   * @param currentDataset
   * @param newDataset
   * @return
   */
  public Optional<String> generateUpdateMessage(Organization currentOrg, Organization newOrg, Dataset currentDataset,
                                                Dataset newDataset) {
    StringJoiner joiner = new StringJoiner(",");
    if(requiresUpdate(currentOrg, newOrg)) {
      joiner.add(MessageFormat.format("Publishing Organization [{0}]: Country [{1}] -> [{2}]", currentOrg.getKey(),
              currentOrg.getCountry(), newOrg.getCountry()));
    }

    if(requiresUpdate(currentDataset, newDataset)) {
      joiner.add(MessageFormat.format("Dataset [{0}]: Publishing Organization [{1}] -> [{2}], " +
              "License [{3}] -> [{4}]", currentDataset.getKey(), currentDataset.getPublishingOrganizationKey(),
              newDataset.getPublishingOrganizationKey(), currentDataset.getLicense(), newDataset.getLicense()));
    }

    return joiner.length() > 0 ? Optional.of(joiner.toString()) : Optional.empty();
  }

  /**
   * Generates a message about what changed in the mutation. Mostly use for logging.
   *
   * @param currentOrg
   * @param newOrg
   * @return
   */
  public Optional<String> generateUpdateMessage(Organization currentOrg, Organization newOrg) {
    StringJoiner joiner = new StringJoiner(",");
    if(requiresUpdate(currentOrg, newOrg)) {
      joiner.add(MessageFormat.format("Publishing Organization [{0}]: Country [{1}] -> [{2}]", currentOrg.getKey(),
                                      currentOrg.getCountry(), newOrg.getCountry()));
    }

    return joiner.length() > 0 ? Optional.of(joiner.toString()) : Optional.empty();
  }

  /**
   * Generates a message about what changed in the mutation. Mostly use for logging.
   *
   * @param currentDataset
   * @param newDataset
   * @param currentDataset
   * @param newDataset
   * @return
   */
  public Optional<String> generateUpdateMessage(Dataset currentDataset, Dataset newDataset) {
    StringJoiner joiner = new StringJoiner(",");
    if(requiresUpdate(currentDataset, newDataset)) {
      joiner.add(MessageFormat.format("Dataset [{0}]: Publishing Organization [{1}] -> [{2}], " +
          "License [{3}] -> [{4}]", currentDataset.getKey(), currentDataset.getPublishingOrganizationKey(),
        newDataset.getPublishingOrganizationKey(), currentDataset.getLicense(), newDataset.getLicense()));
    }

    return joiner.length() > 0 ? Optional.of(joiner.toString()) : Optional.empty();
  }
}
