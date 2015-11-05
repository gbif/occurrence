package org.gbif.occurrence.cli.registry.sync;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.Country;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;
import org.gbif.common.messaging.api.messages.OccurrenceMutatedMessage;
import org.gbif.common.messaging.guice.PostalServiceModule;
import org.gbif.occurrence.cli.registry.RegistryObjectMapperContextResolver;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.guice.OccurrencePersistenceModule;
import org.gbif.registry.ws.client.DatasetWsClient;
import org.gbif.registry.ws.client.OrganizationWsClient;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.apache.ApacheHttpClient;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.map.DeserializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mapreduce Mapper that synchronizes occurrences with the registry. It checks for changed publishing organizations,
 * changed publishing organization country, and dataset deletions. For organization changes the new values are written
 * to the occurrence HBase table via occurrence persistence, and then an OccurrenceMutatedMessage is sent. For dataset
 * deletions a DeleteDatasetMessage is sent.
 */
public class OccurrenceScanMapper extends TableMapper<ImmutableBytesWritable, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceScanMapper.class);

  private static final Set<UUID> DEAD_DATASETS = Sets.newHashSet();
  private static final Set<UUID> UNCHANGED_DATASETS = Sets.newHashSet();
  private static final Map<UUID, Organization> DATASET_TO_OWNING_ORG = Maps.newHashMap();

  private DatasetService datasetService;
  private OrganizationService orgService;
  private OccurrencePersistenceService occurrencePersistenceService;
  private MessagePublisher messagePublisher;

  private int numRecords = 0;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Properties props = new Properties();
    // extract the config properties from the job context
    for (Map.Entry<String, String> entry : context.getConfiguration()) {
      props.setProperty(entry.getKey(), entry.getValue());
    }
    ClientConfig cc = new DefaultClientConfig();
    cc.getClasses().add(JacksonJsonProvider.class);
    cc.getClasses().add(RegistryObjectMapperContextResolver.class);
    cc.getFeatures().put(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES.toString(), false);
    Client httpClient = ApacheHttpClient.create(cc);
    WebResource regResource = httpClient.resource(props.getProperty(SyncCommon.REG_WS_PROPS_KEY));
    datasetService = new DatasetWsClient(regResource, null);
    orgService = new OrganizationWsClient(regResource, null);

    Injector injector =
      Guice.createInjector(new PostalServiceModule("sync", props), new OccurrencePersistenceModule(props));
    occurrencePersistenceService = injector.getInstance(OccurrencePersistenceService.class);
    messagePublisher = injector.getInstance(MessagePublisher.class);
  }

  @Override
  public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
    UUID datasetKey = UUID.fromString(Bytes.toString(values.getValue(SyncCommon.OCC_CF, SyncCommon.DK_COL)));
    if (DEAD_DATASETS.contains(datasetKey) || UNCHANGED_DATASETS.contains(datasetKey)) {
      return;
    }

    Dataset dataset = datasetService.get(datasetKey);
    if (dataset.getDeleted() != null) {
      DEAD_DATASETS.add(datasetKey);
      try {
        LOG.info("Sending delete dataset message for dataset [{}]", datasetKey);
        messagePublisher.send(new DeleteDatasetOccurrencesMessage(datasetKey, OccurrenceDeletionReason.DATASET_MANUAL));
      } catch (IOException e) {
        LOG.warn("Failed to send update message", e);
      }
      return;
    }

    // dataset exists, now compare with values we have on the occurrence
    Organization publishingOrg;

    boolean needsUpdate;
    if (DATASET_TO_OWNING_ORG.containsKey(datasetKey)) {
      // seen it before, no need to do comparisons - record needs updating
      publishingOrg = DATASET_TO_OWNING_ORG.get(datasetKey);
      needsUpdate = true;
    } else {
      UUID newPublishingOrgKey = dataset.getPublishingOrganizationKey();
      publishingOrg = orgService.get(newPublishingOrgKey);
      String rawPublishingOrgKey = Bytes.toString(values.getValue(SyncCommon.OCC_CF, SyncCommon.OOK_COL));
      UUID publishingOrgKey = rawPublishingOrgKey == null ? null : UUID.fromString(rawPublishingOrgKey);
      String rawHostCountry = Bytes.toString(values.getValue(SyncCommon.OCC_CF, SyncCommon.HC_COL));
      Country hostCountry = rawHostCountry == null ? null : Country.fromIsoCode(rawHostCountry);
      Country newHostCountry = publishingOrg.getCountry();
      if (newPublishingOrgKey.equals(publishingOrgKey) && newHostCountry == hostCountry) {
        needsUpdate = false;
        UNCHANGED_DATASETS.add(datasetKey);
      } else {
        needsUpdate = true;
        DATASET_TO_OWNING_ORG.put(datasetKey, publishingOrg);
      }
    }

    if (needsUpdate) {
      Occurrence origOcc = occurrencePersistenceService.get(Bytes.toInt(row.get()));
      // we have no clone or other easy copy method
      Occurrence updatedOcc = occurrencePersistenceService.get(Bytes.toInt(row.get()));
      updatedOcc.setPublishingOrgKey(publishingOrg.getKey());
      updatedOcc.setPublishingCountry(publishingOrg.getCountry());
      occurrencePersistenceService.update(updatedOcc);

      int crawlId = Bytes.toInt(values.getValue(SyncCommon.OCC_CF, SyncCommon.CI_COL));
      OccurrenceMutatedMessage msg =
        OccurrenceMutatedMessage.buildUpdateMessage(datasetKey, origOcc, updatedOcc, crawlId);

      try {
        LOG.info(
          "Sending update for key [{}], publishing org changed from [{}] to [{}] and host country from [{}] to [{}]",
          datasetKey, origOcc.getPublishingOrgKey(), updatedOcc.getPublishingOrgKey(), origOcc.getPublishingCountry(),
          updatedOcc.getPublishingCountry());
        messagePublisher.send(msg);
      } catch (IOException e) {
        LOG.warn("Failed to send update message", e);
      }
    }
    numRecords++;
    if (numRecords % 10000 == 0) {
      context.setStatus("mapper processed " + numRecords + " records so far");
    }
  }
}
