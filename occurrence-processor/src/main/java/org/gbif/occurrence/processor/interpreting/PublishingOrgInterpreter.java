package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.occurrence.processor.interpreting.util.OrganizationLookup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is not really an Interpreter - just a wrapper around the webservice calls to look up the publishing organization of
 * a dataset and its fields.
 */
public class PublishingOrgInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(PublishingOrgInterpreter.class);

  private PublishingOrgInterpreter() {
  }

  public static void interpretPublishingOrg(Occurrence occ) {
    Organization org = OrganizationLookup.getOrgByDataset(occ.getDatasetKey());
    // update the occurrence's publishing org if it's empty or out of sync

    if (org != null && org.getKey() != null && !org.getKey().equals(occ.getPublishingOrgKey())) {
      occ.setPublishingOrgKey(org.getKey());
    }

    if (occ.getPublishingOrgKey() == null) {
      LOG.info("Couldn't find publishing org for occ id [{}] of dataset [{}]", occ.getKey(), occ.getDatasetKey());
    } else {
      Country country = OrganizationLookup.getOrgCountry(occ.getPublishingOrgKey());
      if (country == null) {
        LOG.info("Couldn't find country for publishing org [{}]", occ.getPublishingOrgKey());
      } else {
        occ.setPublishingCountry(country);
      }
    }
  }
}
