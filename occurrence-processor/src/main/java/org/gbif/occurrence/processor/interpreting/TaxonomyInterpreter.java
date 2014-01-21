package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.interpreters.NubLookupInterpreter;
import org.gbif.occurrence.interpreters.TaxonInterpreter;
import org.gbif.occurrence.interpreters.result.NubLookupInterpretationResult;

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
    //TODO: handle case when the scientific name is null and only given as atomized fields: genus & speciesEpitheton
    // see http://dev.gbif.org/issues/browse/POR-1724
    NubLookupInterpretationResult nubLookup = NubLookupInterpreter.nubLookup(
      TaxonInterpreter.mapKingdom(TaxonInterpreter.cleanTaxon(verbatim.getField(DwcTerm.kingdom))),
      TaxonInterpreter.mapPhylum(TaxonInterpreter.cleanTaxon(verbatim.getField(DwcTerm.phylum))),
      TaxonInterpreter.cleanTaxon(verbatim.getField(DwcTerm.class_)),
      TaxonInterpreter.cleanTaxon(verbatim.getField(DwcTerm.order)),
      TaxonInterpreter.cleanTaxon(verbatim.getField(DwcTerm.family)),
      TaxonInterpreter.cleanTaxon(verbatim.getField(DwcTerm.genus)),
      TaxonInterpreter.parseName(TaxonInterpreter.cleanTaxon(verbatim.getField(DwcTerm.scientificName))),
      TaxonInterpreter.cleanAuthor(verbatim.getField(DwcTerm.scientificNameAuthorship)));

    if (nubLookup == null) {
      LOG.debug("Got null nubLookup for sci name [{}]", occ.getScientificName());
    } else {
      occ.setTaxonKey(nubLookup.getUsageKey());
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
