package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.registry.ws.client.DatasetWsClient;
import org.gbif.registry.ws.client.OrganizationWsClient;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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

  private static final Logger LOG = LoggerFactory.getLogger(DatasetInfoInterpreter.class);

  private final DatasetWsClient datasetClient;
  private final OrganizationWsClient orgClient;

  // The repetitive nature of our data encourages use of a light cache to reduce WS load
  private final LoadingCache<UUID, Dataset> datasetCache =
    CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(1, TimeUnit.MINUTES)
      .build(new CacheLoader<UUID, Dataset>() {

        @Override
        public Dataset load(UUID key) throws Exception {
          return datasetClient.get(key);
        }
      });

  private final LoadingCache<UUID, Organization> orgCache =
    CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(1, TimeUnit.MINUTES)
      .build(new CacheLoader<UUID, Organization>() {

        @Override
        public Organization load(UUID key) throws Exception {
          return orgClient.get(key);
        }
      });

  @Inject
  public DatasetInfoInterpreter(WebResource apiWs) {
    datasetClient = new DatasetWsClient(apiWs, null);
    orgClient = new OrganizationWsClient(apiWs, null);
  }

  public void interpretDatasetInfo(Occurrence occ) {
    Organization org = getOrgByDataset(occ.getDatasetKey());
    // update the occurrence's publishing org if it's empty or out of sync

    if (org != null && org.getKey() != null && !org.getKey().equals(occ.getPublishingOrgKey())) {
      occ.setPublishingOrgKey(org.getKey());
    }

    if (occ.getPublishingOrgKey() == null) {
      LOG.info("Couldn't find publishing org for occ id [{}] of dataset [{}]", occ.getKey(), occ.getDatasetKey());
    } else {
      Country country = getOrgCountry(occ.getPublishingOrgKey());
      if (country == null) {
        LOG.info("Couldn't find country for publishing org [{}]", occ.getPublishingOrgKey());
      } else {
        occ.setPublishingCountry(country);
      }
    }

    License license = getDatasetLicense(occ.getDatasetKey());
    if (license != null) {
      occ.setLicense(license);
    }
  }

  /**
   * Find and return the organization which publishes the dataset for the given datasetKey.
   *
   * @param datasetKey the dataset publisher to find
   * @return the organization that publishes the dataset
   */
  @Nullable
  @VisibleForTesting
  protected Organization getOrgByDataset(UUID datasetKey) {
    checkNotNull(datasetKey, "datasetKey can't be null");

    Organization org = null;
    try {
      Dataset dataset = datasetCache.get(datasetKey);
      if (dataset != null && dataset.getPublishingOrganizationKey() != null) {
        org = orgCache.get(dataset.getPublishingOrganizationKey());
      }
    } catch (UncheckedExecutionException | ExecutionException e) {
      LOG.warn("WS failure while looking up org for dataset [{}]", datasetKey, e);
    }

    return org == null ? null : org;
  }

  /**
   * Find and return the organization which publishes the dataset for the given datasetKey.
   *
   * @param datasetKey the dataset publisher to find
   * @return the dataset license
   */
  @Nullable
  @VisibleForTesting
  protected License getDatasetLicense(UUID datasetKey) {
    checkNotNull(datasetKey, "datasetKey can't be null");

    License license = null;
    try {
      Dataset dataset = datasetCache.get(datasetKey);
      if (dataset != null && dataset.getLicense() != null) {
        license = dataset.getLicense();
      }
    } catch (UncheckedExecutionException | ExecutionException e) {
      LOG.warn("WS failure while looking up org for dataset [{}]", datasetKey, e);
    }

    return license == null ? null : license;
  }

  /**
   * Find and return the country of the organization.
   *
   * @param organizationKey the organization for which to find the country
   * @return the org's country (the "hostCountry") or null if the organization couldn't be found (or loaded)
   */
  @Nullable
  @VisibleForTesting
  protected Country getOrgCountry(UUID organizationKey) {
    checkNotNull(organizationKey, "organizationKey can't be null");

    Organization org = null;
    try {
      org = orgCache.get(organizationKey);
    } catch (UncheckedExecutionException | ExecutionException e) {
      LOG.warn("WS failure while looking up country for org [{}]", organizationKey, e);
    }

    return org == null ? null : org.getCountry();
  }

}
