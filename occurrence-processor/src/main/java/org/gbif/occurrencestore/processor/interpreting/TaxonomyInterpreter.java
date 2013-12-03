package org.gbif.occurrencestore.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.occurrencestore.interpreters.NubLookupInterpreter;
import org.gbif.occurrencestore.interpreters.TaxonInterpreter;
import org.gbif.occurrencestore.interpreters.result.NubLookupInterpretationResult;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaxonomyInterpreter implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpreter.class);

  private VerbatimOccurrence verbatim;
  private Occurrence occ;

  public TaxonomyInterpreter(VerbatimOccurrence verbatim, Occurrence occ) {
    this.verbatim = verbatim;
    this.occ = occ;
  }

  @Override
  public void run() {
    NubLookupInterpretationResult nubLookup = NubLookupInterpreter
      .nubLookup(TaxonInterpreter.mapKingdom(TaxonInterpreter.cleanTaxon(verbatim.getKingdom())),
        TaxonInterpreter.mapPhylum(TaxonInterpreter.cleanTaxon(verbatim.getPhylum())),
        TaxonInterpreter.cleanTaxon(verbatim.getKlass()), TaxonInterpreter.cleanTaxon(verbatim.getOrder()),
        TaxonInterpreter.cleanTaxon(verbatim.getFamily()), TaxonInterpreter.cleanTaxon(verbatim.getGenus()),
        TaxonInterpreter.parseName(TaxonInterpreter.cleanTaxon(verbatim.getScientificName())),
        TaxonInterpreter.cleanAuthor(verbatim.getAuthor()));

    if (nubLookup == null) {
      LOG.debug("Got null nubLookup for sci name [{}]", occ.getScientificName());
    } else {
      occ.setNubKey(nubLookup.getUsageKey());
      occ.setKingdomKey(nubLookup.getKingdomKey());
      occ.setPhylumKey(nubLookup.getPhylumKey());
      occ.setClassKey(nubLookup.getClassKey());
      occ.setOrderKey(nubLookup.getOrderKey());
      occ.setFamilyKey(nubLookup.getFamilyKey());
      occ.setGenusKey(nubLookup.getGenusKey());
      occ.setSpeciesKey(nubLookup.getSpeciesKey());
      occ.setKingdom(nubLookup.getKingdom());
      occ.setPhylum(nubLookup.getPhylum());
      occ.setClazz(nubLookup.getClazz());
      occ.setOrder(nubLookup.getOrder());
      occ.setFamily(nubLookup.getFamily());
      occ.setGenus(nubLookup.getGenus());
      occ.setSpecies(nubLookup.getSpecies());
      occ.setScientificName(nubLookup.getScientificName());
      LOG.debug("Got nub sci name [{}]", occ.getScientificName());
    }
  }
}
