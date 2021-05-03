package org.gbif.occurrence.cli.registry.sync;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Installation;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.model.registry.eml.Project;

import java.text.MessageFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * The class is responsible to know the rules to determine if an Occurrence record should be updated or not after
 * a change coming from the Registry. It is also responsible to "mutate" an Occurrence object to its new state.
 */
public class RegistryBasedOccurrenceMutator {

  /**
   * Only one of two objects is null.
   */
  private static boolean nullChange(Object o1, Object o2) {
    return (o1 == null && o2 != null) || (o1 != null && o2 == null);
  }

  /**
   * Are all objects non null.
   */
  private static boolean nonNull(Object... elements) {
    return Arrays.stream(elements).allMatch(Objects::nonNull);
  }

  /**
   * Has the project changed.
   */
  private boolean datasetProjectChanged(Project currentProject, Project newProject) {
    return nullChange(currentProject, newProject) ||
           (nonNull(currentProject, newProject) &&
            !Objects.equals(currentProject.getIdentifier(), newProject.getIdentifier()));
  }

  /**
   * Transforms a list to a set, null-aware.
   */
  private <T> Set<T> toSet(List<T> l) {
    return Objects.isNull(l)? Collections.emptySet() : new HashSet<>(l);
  }

  /**
   * Has the network keys changed.
   */
  private boolean datasetNetworkChanged(List<UUID> currentNetworks, List<UUID> newNetworks) {
    return (nullChange(currentNetworks, newNetworks) && nonNull(currentNetworks, newNetworks))
           || !toSet(currentNetworks).equals(toSet(newNetworks));
  }

  /**
   * Has the dataset title changed?.
   */
  private boolean datasetTitleChanged(Dataset currentDataset, Dataset newDataset) {
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
   * Check if changes on a dataset should trigger an update of Occurrence records.
   */
  public boolean requiresUpdate(Dataset currentDataset, Dataset newDataset) {
    //Changes that trigger indexing: organzation, project, title, installations and networks
    return !Objects.equals(currentDataset.getPublishingOrganizationKey(), newDataset.getPublishingOrganizationKey()) ||
            datasetProjectChanged(currentDataset.getProject(), newDataset.getProject()) ||
            !Objects.equals(currentDataset.getTitle(), newDataset.getTitle()) ||
            datasetNetworkChanged(currentDataset.getNetworkKeys(), newDataset.getNetworkKeys()) ||
            !Objects.equals(currentDataset.getInstallationKey(), newDataset.getInstallationKey()) ||
            datasetTitleChanged(currentDataset, newDataset);

  }

  /**
   * Check if changes on an organization should trigger an update of Occurrence records of all its datasets.
   */
  public boolean requiresUpdate(Organization currentOrg, Organization newOrg) {
    // endorsement not approved means that we don't have records so nothing to update
    if (!newOrg.isEndorsementApproved()) {
      return false;
    }

    //Country or title changed
    return !Objects.equals(currentOrg.getCountry(), newOrg.getCountry()) ||
           !Objects.equals(currentOrg.getTitle(), newOrg.getTitle());
  }

  /**
   * Check if changes on an organization should trigger an update of Occurrence records of all its datasets.
   */
  public boolean requiresUpdate(Installation currentInstallation, Installation newInstallation) {
    //Country or title changed
    return !Objects.equals(currentInstallation.getOrganizationKey(), currentInstallation.getOrganizationKey());
  }

  /**
   * Generates a message about what changed in the mutation. Mostly use for logging.
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
