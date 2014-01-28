package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.registry.ws.client.DatasetWsClient;
import org.gbif.registry.ws.client.OrganizationWsClient;
import org.gbif.ws.json.JacksonJsonContextResolver;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache.ApacheHttpClient;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Static utility to return fields from an organization.
 */
public class OrganizationLookup {

  private static final Logger LOG = LoggerFactory.getLogger(OrganizationLookup.class);

  private static final String WEB_SERVICE_URL;
  private static final String WEB_SERVICE_URL_PROPERTY = "registry.ws.url";
  private static final String OCCURRENCE_PROPS_FILE = "occurrence-processor.properties";
  private static final DatasetWsClient DATASET_CLIENT;
  private static final OrganizationWsClient ORG_CLIENT;

  // The repetitive nature of our data encourages use of a light cache to reduce WS load
  private static final LoadingCache<UUID, Dataset> DATASET_CACHE =
    CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(1, TimeUnit.MINUTES)
      .build(new CacheLoader<UUID, Dataset>() {

        @Override
        public Dataset load(UUID key) throws Exception {
          return DATASET_CLIENT.get(key);
        }
      });

  private static final LoadingCache<UUID, Organization> ORG_CACHE =
    CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(1, TimeUnit.MINUTES)
      .build(new CacheLoader<UUID, Organization>() {

        @Override
        public Organization load(UUID key) throws Exception {
          return ORG_CLIENT.get(key);
        }
      });

  private static final WebResource RESOURCE;

  static {
    try {
      InputStream is = NubLookupInterpreter.class.getClassLoader().getResourceAsStream(OCCURRENCE_PROPS_FILE);
      if (is == null) {
        throw new RuntimeException("Can't load properties file [" + OCCURRENCE_PROPS_FILE + ']');
      }
      try {
        Properties props = new Properties();
        props.load(is);
        WEB_SERVICE_URL = props.getProperty(WEB_SERVICE_URL_PROPERTY);
      } finally {
        is.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't load properties file [" + OCCURRENCE_PROPS_FILE + ']', e);
    }

    ClientConfig cc = new DefaultClientConfig();
    cc.getClasses().add(JacksonJsonContextResolver.class);
    cc.getClasses().add(JacksonJsonProvider.class);
    cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    Client client = ApacheHttpClient.create(cc);
    RESOURCE = client.resource(WEB_SERVICE_URL);
    DATASET_CLIENT = new DatasetWsClient(RESOURCE, null);
    ORG_CLIENT = new OrganizationWsClient(RESOURCE, null);
  }

  private OrganizationLookup() {
  }

  /**
   * Find and return the organization which owns the dataset for the given datasetKey.
   *
   * @param datasetKey the dataset owner to find
   * @return the organization that owns the dataset
   */
  @Nullable
  public static Organization getOrgByDataset(UUID datasetKey) {
    checkNotNull(datasetKey, "datasetKey can't be null");

    Organization org = null;
    try {
      Dataset dataset = DATASET_CACHE.get(datasetKey);
      if (dataset != null && dataset.getOwningOrganizationKey() != null) {
        org = ORG_CACHE.get(dataset.getOwningOrganizationKey());
      }
    } catch (UncheckedExecutionException e) {
      LOG.warn("WS failure while looking up org for dataset [{}]", datasetKey, e);
    } catch (ExecutionException e) {
      LOG.warn("WS failure while looking up org for dataset [{}]", datasetKey, e);
    }

    return org == null ? null : org;
  }

  /**
   * Find and return the country of the organization.
   *
   * @param organizationKey the organization for which to find the country
   * @return the org's country (the "hostCountry") or null if the organization couldn't be found (or loaded)
   */
  @Nullable
  public static Country getOrgCountry(UUID organizationKey) {
    checkNotNull(organizationKey, "organizationKey can't be null");

    Organization org = null;
    try {
      org = ORG_CACHE.get(organizationKey);
    } catch (UncheckedExecutionException e) {
      LOG.warn("WS failure while looking up country for org [{}]", organizationKey, e);
    } catch (ExecutionException e) {
      LOG.warn("WS failure while looking up country for org [{}]", organizationKey, e);
    }

    return org == null ? null : org.getCountry();
  }
}
