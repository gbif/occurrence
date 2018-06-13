package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Network;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.registry.ws.client.DatasetWsClient;
import org.gbif.registry.ws.client.OrganizationWsClient;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import com.sun.jersey.api.client.WebResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is not an Interpreter. It's just a wrapper around the webservice calls to look up dataset info that is included
 * in occurrence records.
 */
public class DatasetInfoInterpreter implements Serializable {

  @VisibleForTesting
  protected static class DatasetCacheData {

    private final Dataset dataset;
    private final List<Network> networks;
    private final Organization organization;

    public DatasetCacheData(Dataset dataset, List<Network> networks, Organization organization) {
      this.dataset = dataset;
      this.networks = networks;
      this.organization = organization;
    }

      public Dataset getDataset() {
          return dataset;
      }

      public List<Network> getNetworks() {
          return networks;
      }

      public Organization getOrganization() {
          return organization;
      }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DatasetInfoInterpreter.class);

  private static final UUID EBIRD_DATASET = UUID.fromString("4fa7b334-ce0d-4e88-aaae-2e0c138d049e");

  private static final CountryParser COUNTRY_PARSER = CountryParser.getInstance();

  private final DatasetWsClient datasetClient;
  private final OrganizationWsClient orgClient;


  private final LoadingCache<UUID, Organization> orgCache =
    CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(1, TimeUnit.MINUTES)
      .build(new CacheLoader<UUID, Organization>() {

        @Override
        public Organization load(UUID key) throws Exception {
          return orgClient.get(key);
       }
    });

  // The repetitive nature of our data encourages use of a light cache to reduce WS load
  private final LoadingCache<UUID, DatasetCacheData> datasetCache =
    CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(1, TimeUnit.MINUTES)
      .build(new CacheLoader<UUID, DatasetCacheData>() {

        @Override
        public DatasetCacheData load(UUID key) throws Exception {
          Dataset dataset = datasetClient.get(key);
          if (dataset != null) {
            return new DatasetCacheData(dataset, datasetClient.listNetworks(key),
                                        orgCache.get(dataset.getPublishingOrganizationKey()));
          }
          return null;
        }
      });



  @Inject
  public DatasetInfoInterpreter(WebResource apiWs) {
    datasetClient = new DatasetWsClient(apiWs, null);
    orgClient = new OrganizationWsClient(apiWs, null);
  }

  public void interpretDatasetInfo(Occurrence occ) {

    DatasetCacheData datasetCacheData = getDatasetData(occ.getDatasetKey());
    if (datasetCacheData == null) {
      LOG.error("Couldn't find dataset key [{}] for occ id [{}]", occ.getKey(), occ.getDatasetKey());
      return;
    }

    // update the occurrence's publishing org if it's empty or out of sync
    if (occ.getPublishingOrgKey() == null) {
      LOG.info("Couldn't find publishing org for occ id [{}] of dataset [{}]", occ.getKey(), occ.getDatasetKey());
    } else {
      Organization org = datasetCacheData.organization;
      // Special case for eBird, use the supplied publishing country.
      if (!occ.getPublishingOrgKey().equals(org.getKey())) {
        occ.setPublishingOrgKey(org.getKey());
      }
      Country country = null;
      if (occ.getDatasetKey().equals(EBIRD_DATASET)) {
        String verbatimPublishingCountryCode = occ.getVerbatimField(GbifTerm.publishingCountry);

        OccurrenceParseResult<Country> result = new OccurrenceParseResult<>(COUNTRY_PARSER.parse(verbatimPublishingCountryCode));

        if (result.isSuccessful()) {
          country = result.getPayload();
        } else {
          LOG.info("Couldn't find publishing country for eBird record [{}]", occ.getKey());
        }
      } else {
        country = org.getCountry();
      }

      if (country == null) {
        LOG.info("Couldn't find country for publishing org [{}]", occ.getPublishingOrgKey());
      } else {
        occ.setPublishingCountry(country);
      }
    }

    Optional.ofNullable(datasetCacheData.dataset.getLicense()).ifPresent(occ::setLicense);
    Optional.ofNullable(datasetCacheData.dataset.getInstallationKey()).ifPresent(occ::setInstallationKey);
    Optional.ofNullable(datasetCacheData.networks).ifPresent(networks ->
        occ.setNetworkKeys(networks.stream().map(Network::getKey).collect(Collectors.toList()))
    );
  }

  /**
   * Find and return the organization which publishes the dataset for the given datasetKey.
   *
   * @param datasetKey the dataset publisher to find
   * @return the organization that publishes the dataset
   */
  @Nullable
  @VisibleForTesting
  protected DatasetCacheData getDatasetData(UUID datasetKey) {
    checkNotNull(datasetKey, "datasetKey can't be null");
    DatasetCacheData datasetCacheData = null;
    try {
      return datasetCache.get(datasetKey);
    } catch (UncheckedExecutionException | ExecutionException e) {
      LOG.warn("WS failure while looking up org for dataset [{}]", datasetKey, e);
    }

    return datasetCacheData;
  }


}
