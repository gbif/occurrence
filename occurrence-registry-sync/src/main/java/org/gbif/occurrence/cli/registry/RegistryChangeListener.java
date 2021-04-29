package org.gbif.occurrence.cli.registry;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.Installation;
import org.gbif.api.model.registry.Network;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.api.service.registry.NetworkService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.util.comparators.EndpointPriorityComparator;
import org.gbif.api.util.iterables.Iterables;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.*;
import org.gbif.occurrence.cli.registry.sync.RegistryBasedOccurrenceMutator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens for any registry changes {@link RegistryChangeMessage}
 * of interest to occurrences: namely organization and dataset updates or deletions.
 * <p>
 * This was written at a time when we only looked at occurrence datasets, but without
 * planning, it is now the process that also triggers crawling for checklist datasets,
 * and metadata-only datasets.
 */
public class RegistryChangeListener extends AbstractMessageCallback<RegistryChangeMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(RegistryChangeListener.class);
  private static final int PAGING_LIMIT = 20;

  private static final EnumSet<EndpointType> CRAWLABLE_ENDPOINT_TYPES = EnumSet.of(
    EndpointType.BIOCASE,
    EndpointType.DIGIR,
    EndpointType.DIGIR_MANIS,
    EndpointType.TAPIR,
    EndpointType.DWC_ARCHIVE,
    EndpointType.EML);

  // pipelines
  private static final String METADATA_INTERPRETATION = "METADATA";
  private static final String LOCATION_INTERPRETATION = "LOCATION";

  /*
    When an IPT publishes a new dataset we will get multiple messages from the registry informing us of the update
    (depending on the number of endpoints, contacts etc). We only want to send a single crawl message for one of those
    updates so we cache the dataset uuid for 5 seconds, which should be long enough to handle all of the registry
    updates.
    */
  private static final Cache<UUID, Object> RECENTLY_UPDATED_DATASETS =
    CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.SECONDS).initialCapacity(10).maximumSize(1000).build();
  // used as a value for the cache - we only care about the keys
  private static final Object EMPTY_VALUE = new Object();

  private final MessagePublisher messagePublisher;
  private final OrganizationService orgService;
  private final NetworkService networkService;
  private final InstallationService installationService;
  private RegistryBasedOccurrenceMutator occurrenceMutator;

  public RegistryChangeListener(MessagePublisher messagePublisher, OrganizationService orgService,
                                NetworkService networkService, InstallationService installationService) {
    this.messagePublisher = messagePublisher;
    this.orgService = orgService;
    this.occurrenceMutator = new RegistryBasedOccurrenceMutator();
    this.networkService = networkService;
    this.installationService = installationService;
  }

  @Override
  public void handleMessage(RegistryChangeMessage message) {
    LOG.info("Handling registry [{}] msg for class [{}]", message.getChangeType(), message.getObjectClass());
    Class<?> clazz = message.getObjectClass();
    if ("Dataset".equals(clazz.getSimpleName())) {
      handleDataset(message.getChangeType(), (Dataset) message.getOldObject(), (Dataset) message.getNewObject());
    } else if ("Organization".equals(clazz.getSimpleName())) {
      handleOrganization(message.getChangeType(),
                         (Organization) message.getOldObject(),
                         (Organization) message.getNewObject());
    } else if ("Network".equals(clazz.getSimpleName())) {
      handleNetwork(message.getChangeType(),
                         (Network) message.getOldObject(),
                         (Network) message.getNewObject());
    } else if ("Installation".equals(clazz.getSimpleName())) {
      handleInstallation(message.getChangeType(),
                        (Installation) message.getOldObject(),
                        (Installation) message.getNewObject());
    }
  }

  private void handleDataset(RegistryChangeMessage.ChangeType changeType, Dataset oldDataset, Dataset newDataset) {
    switch (changeType) {
      case DELETED:
        LOG.info("Sending delete for dataset [{}]", oldDataset.getKey());
        try {
          messagePublisher.send(new DeleteDatasetOccurrencesMessage(oldDataset.getKey(),
                                                                    OccurrenceDeletionReason.DATASET_MANUAL));
        } catch (IOException e) {
          LOG.warn("Could not send delete dataset message for key [{}]", oldDataset.getKey(), e);
        }
        break;
      case UPDATED:
        // if it has a crawlable endpoint and we haven't just sent a crawl msg, we need to crawl it no matter what changed
        if (shouldCrawl(newDataset)) {
          LOG.info("Sending crawl for updated dataset [{}]", newDataset.getKey());
          try {
            messagePublisher.send(new StartCrawlMessage(newDataset.getKey()));
          } catch (IOException e) {
            LOG.warn("Could not send start crawl message for dataset key [{}]", newDataset.getKey(), e);
          }
          // check if we should start a m/r job to update occurrence records
          if (occurrenceMutator.requiresUpdate(oldDataset, newDataset)) {
            LOG.info("Sending medatada update for dataset [{}]", newDataset.getKey());
            Optional<String> changedMessage = occurrenceMutator.generateUpdateMessage(oldDataset, newDataset);

            // send message to pipelines
            sendUpdateMessageToPipelines(
                newDataset, Collections.singleton(METADATA_INTERPRETATION), changedMessage.orElse("Dataset changed in registry"));

          } else {
            LOG.debug("Owning orgs and license match for updated dataset [{}] - taking no action", newDataset.getKey());
          }
        } else {
          LOG.info("Ignoring update of dataset [{}] because either no crawlable endpoints or we just sent a crawl",
                   newDataset.getKey());
        }
        break;
      case CREATED:
        if (shouldCrawl(newDataset)) {
          LOG.info("Sending crawl for new dataset [{}]", newDataset.getKey());
          try {
            messagePublisher.send(new StartCrawlMessage(newDataset.getKey()));
          } catch (IOException e) {
            LOG.warn("Could not send start crawl message for dataset key [{}]", newDataset.getKey(), e);
          }
        } else {
          LOG.info("Ignoring creation of dataset [{}] because no crawlable endpoints or we just sent a crawl",
                   newDataset.getKey());
        }
        break;
    }
  }

  private static boolean shouldCrawl(Dataset dataset) {
    if (RECENTLY_UPDATED_DATASETS.getIfPresent(dataset.getKey()) == null && isCrawlable(dataset)) {
      RECENTLY_UPDATED_DATASETS.put(dataset.getKey(), EMPTY_VALUE);
      return true;
    }

    return false;
  }

  private static boolean isCrawlable(Dataset dataset) {
    for (Endpoint endpoint : dataset.getEndpoints()) {
      if (CRAWLABLE_ENDPOINT_TYPES.contains(endpoint.getType())) {
        return true;
      }
    }

    return false;
  }

  private void handleOrganization(
    RegistryChangeMessage.ChangeType changeType, Organization oldOrg, Organization newOrg
  ) {
    switch (changeType) {
      case UPDATED:
        if (!oldOrg.isEndorsementApproved() && newOrg.isEndorsementApproved()) {
          LOG.info("Starting crawl of all datasets for newly endorsed org [{}]", newOrg.getKey());
          DatasetVisitor visitor = dataset -> {
            try {
              messagePublisher.send(new StartCrawlMessage(dataset.getKey()));
            } catch (IOException e) {
              LOG.warn("Could not send start crawl message for newly endorsed dataset key [{}]", dataset.getKey(), e);
            }
          };
          visitOwnedDatasets(newOrg.getKey(), visitor);
        } else if (occurrenceMutator.requiresUpdate(oldOrg, newOrg)
            && newOrg.getNumPublishedDatasets() > 0) {
          LOG.info(
              "Starting ingestion of all datasets of org [{}] because it has changed country from [{}] to [{}]",
              newOrg.getKey(),
              oldOrg.getCountry(),
              newOrg.getCountry());
          DatasetVisitor visitor =
              dataset -> {
                sendUpdateMessageToPipelines(
                    dataset,
                    Sets.newHashSet(METADATA_INTERPRETATION, LOCATION_INTERPRETATION),
                    occurrenceMutator
                        .generateUpdateMessage(oldOrg, newOrg)
                        .orElse("Organization change in registry"));
              };
          visitOwnedDatasets(newOrg.getKey(), visitor);
        }
        break;
      case DELETED:
      case CREATED:
        break;
    }
  }

  private void handleNetwork(RegistryChangeMessage.ChangeType changeType, Network oldNetwork, Network newNetwork) {
    switch (changeType) {
      case UPDATED:
        if (occurrenceMutator.requiresUpdate(oldNetwork, newNetwork)) {
          LOG.info("Network changed {}", newNetwork.getKey());
          DatasetVisitor visitor = dataset -> sendUpdateMessageToPipelines(dataset,
                                                                           Collections.singleton(METADATA_INTERPRETATION),
                                                                           "Network change " + newNetwork.getKey());
          visitNetworkDatasets(newNetwork.getKey(), visitor);
        }
        break;
      case DELETED:
      case CREATED:
        break;
    }
  }

  private void handleInstallation(RegistryChangeMessage.ChangeType changeType, Installation oldInstallation, Installation newInstallation) {
    switch (changeType) {
      case UPDATED:
        if (occurrenceMutator.requiresUpdate(oldInstallation, newInstallation)) {
          LOG.info("Installation changed {}", newInstallation.getKey());
          DatasetVisitor visitor =
            dataset -> {
              sendUpdateMessageToPipelines(
                dataset,
                Collections.singleton(METADATA_INTERPRETATION),
                "Installation change " + newInstallation.getKey());
            };
          visitInstallationsDataset(newInstallation.getKey(), visitor);
        }
        break;
      case DELETED:
      case CREATED:
        break;
    }
  }

  private void visitOwnedDatasets(UUID orgKey, DatasetVisitor visitor) {
    AtomicInteger datasetCount = new AtomicInteger(0);
    Iterables.publishedDatasets(orgKey, null, orgService)
      .forEach(dataset -> {
        visitor.visit(dataset);
        datasetCount.incrementAndGet();
      });
    LOG.info("Visited [{}] datasets owned by org [{}]", datasetCount.get(), orgKey);
  }

  private void visitNetworkDatasets(UUID networkKey, DatasetVisitor visitor) {
    AtomicInteger datasetCount = new AtomicInteger(0);
    Iterables.networkDatasets(networkKey, null, networkService)
      .forEach(dataset -> {
        visitor.visit(dataset);
        datasetCount.incrementAndGet();
      });
    LOG.info("Visited [{}] datasets in network [{}]", datasetCount.get(), networkKey);
  }

  private void visitInstallationsDataset(UUID installationKey, DatasetVisitor visitor) {
    AtomicInteger datasetCount = new AtomicInteger(0);
    Iterables.hostedDatasets(installationKey, null, installationService)
      .forEach(dataset -> {
        visitor.visit(dataset);
        datasetCount.incrementAndGet();
      });
    LOG.info("Visited [{}] datasets hosted by installation [{}]", datasetCount.get(), installationKey);
  }

  private interface DatasetVisitor {

    void visit(Dataset dataset);
  }

  /**
   * Sends message to pipelines to update the metadata of the dataset.
   *
   * @param dataset dataset to update
   * @param interpretations interpretations that should be run
   * @param changedMessage message with the change occurred in the registry
   */
  private void sendUpdateMessageToPipelines(Dataset dataset, Set<String> interpretations, String changedMessage) {
    Optional<Endpoint> endpoint = getEndpoint(dataset);
    if (!endpoint.isPresent()) {
      LOG.error(
        "Could not find a valid endpoint for dataset {}. Message to pipelines to update metadata NOT SENT",
        dataset.getKey());
      return;
    }

    LOG.info(
        "Sending a message to pipelines to update the {} for dataset [{}], with reason {}",
        interpretations,
        dataset.getKey(),
        changedMessage);

    try {
      PipelinesVerbatimMessage message =
          new PipelinesVerbatimMessage(
              dataset.getKey(),
              null,
              interpretations,
              Sets.newHashSet("VERBATIM_TO_INTERPRETED", "INTERPRETED_TO_INDEX", "HDFS_VIEW"),
              endpoint.get().getType());

      messagePublisher.send(
        new PipelinesBalancerMessage(message.getClass().getSimpleName(), message.toString()));
    } catch (IOException e) {
      LOG.error("Could not send message to pipelines to update metadata for dataset [{}]", dataset.getKey(), e);
    }
  }

  private Optional<Endpoint> getEndpoint(Dataset dataset) {
    return dataset.getEndpoints()
      .stream()
      .filter(e -> EndpointPriorityComparator.PRIORITIES.contains(e.getType()))
      .max(new EndpointPriorityComparator());
  }
}
