package org.gbif.occurrencestore.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.occurrencestore.interpreters.OrganizationLookup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is not really an Interpreter - just a wrapper around the webservice calls to look up the owning organization of
 * a dataset and its fields.
 */
public class OwningOrgInterpreter implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(OwningOrgInterpreter.class);

  private final Occurrence occ;

  public OwningOrgInterpreter(Occurrence occ) {
    this.occ = occ;
  }

  @Override
  public void run() {
    Organization org = OrganizationLookup.getOrgByDataset(occ.getDatasetKey());
    // update the occurrence's owning org if it's empty or out of sync
    if (org != null && !org.getKey().equals(occ.getOwningOrgKey())) {
      occ.setOwningOrgKey(org.getKey());
    }

    if (occ.getOwningOrgKey() == null) {
      LOG.info("Couldn't find owning org for occ id [{}] of dataset [{}]", occ.getKey(), occ.getDatasetKey());
    } else {
      Country country = OrganizationLookup.getOrgCountry(occ.getOwningOrgKey());
      if (country == null) {
        LOG.info("Couldn't find country for owning org [{}]", occ.getOwningOrgKey());
      } else {
        occ.setHostCountry(country);
      }
    }
  }
}
