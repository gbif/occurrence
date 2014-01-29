package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.processor.interpreting.util.NubLookupInterpreter;

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
    String sciname = ClassificationUtils.clean(verbatim.getField(DwcTerm.scientificName));
    if (sciname == null) {
      // handle case when the scientific name is null and only given as atomized fields: genus & speciesEpitheton
      ParsedName pn = new ParsedName();
      pn.setGenusOrAbove(verbatim.getField(DwcTerm.genus));
      pn.setSpecificEpithet(verbatim.getField(DwcTerm.specificEpithet));
      pn.setInfraSpecificEpithet(verbatim.getField(DwcTerm.infraspecificEpithet));
      sciname = pn.canonicalName();
    }

    ParseResult<NameUsageMatch> nubLookup = NubLookupInterpreter.nubLookup(
      ClassificationUtils.clean(verbatim.getField(DwcTerm.kingdom)),
      ClassificationUtils.clean(verbatim.getField(DwcTerm.phylum)),
      ClassificationUtils.clean(verbatim.getField(DwcTerm.class_)),
      ClassificationUtils.clean(verbatim.getField(DwcTerm.order)),
      ClassificationUtils.clean(verbatim.getField(DwcTerm.family)),
      ClassificationUtils.clean(verbatim.getField(DwcTerm.genus)),
      sciname,
      ClassificationUtils.cleanAuthor(verbatim.getField(DwcTerm.scientificNameAuthorship)));

    if (nubLookup == null || nubLookup.getPayload() == null) {
      LOG.debug("Got null nubLookup for sci name [{}]", occ.getScientificName());

    } else {
      NameUsageMatch match = nubLookup.getPayload();
      occ.setTaxonKey(nubLookup.getPayload().getUsageKey());
      occ.setScientificName(nubLookup.getPayload().getScientificName());
      for (Rank r : Rank.DWC_RANKS) {
        org.gbif.api.util.ClassificationUtils.setHigherRank(occ, r, match.getHigherRank(r));
        org.gbif.api.util.ClassificationUtils.setHigherRankKey(occ, r, match.getHigherRankKey(r));
      }
      LOG.debug("Got nub sci name [{}]", occ.getScientificName());
    }
  }
}
