package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.nameparser.NameParser;
import org.gbif.nameparser.UnparsableException;
import org.gbif.occurrence.processor.interpreting.util.NubLookupInterpreter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a VerbatimOccurrence and does nub lookup on its provided taxonomy, then writes the result to the passed in
 * Occurrence.
 */
public class TaxonomyInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpreter.class);
  private static final NameParser parser = new NameParser();

  private TaxonomyInterpreter() {
  }

  public static void interpretTaxonomy(VerbatimOccurrence verbatim, Occurrence occ) {
    String sciname = ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.scientificName));
    if (sciname == null) {
      // handle case when the scientific name is null and only given as atomized fields: genus & speciesEpitheton
      ParsedName pn = new ParsedName();
      if (verbatim.hasVerbatimField(DwcTerm.genericName)) {
        pn.setGenusOrAbove(verbatim.getVerbatimField(DwcTerm.genericName));
      } else {
        pn.setGenusOrAbove(verbatim.getVerbatimField(DwcTerm.genus));
      }
      pn.setSpecificEpithet(verbatim.getVerbatimField(DwcTerm.specificEpithet));
      pn.setInfraSpecificEpithet(verbatim.getVerbatimField(DwcTerm.infraspecificEpithet));
      sciname = pn.canonicalName();
    }

    ParseResult<NameUsageMatch> nubLookup = NubLookupInterpreter.nubLookup(
      ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.kingdom)),
      ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.phylum)),
      ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.class_)),
      ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.order)),
      ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.family)),
      ClassificationUtils.clean(verbatim.getVerbatimField(DwcTerm.genus)),
      sciname,
      ClassificationUtils.cleanAuthor(verbatim.getVerbatimField(DwcTerm.scientificNameAuthorship)));

    if (nubLookup == null || nubLookup.getPayload() == null) {
      LOG.debug("Got null nubLookup for sci name [{}]", occ.getScientificName());

    } else {
      NameUsageMatch match = nubLookup.getPayload();
      occ.setTaxonKey(nubLookup.getPayload().getUsageKey());
      occ.setScientificName(nubLookup.getPayload().getScientificName());
      occ.setTaxonRank(nubLookup.getPayload().getRank());
      // parse name into pieces - we dont get them from the nub lookup
      try {
        ParsedName pn = parser.parse(occ.getScientificName());
        occ.setGenericName(pn.getGenusOrAbove());
        occ.setSpecificEpithet(pn.getSpecificEpithet());
        occ.setInfraspecificEpithet(pn.getInfraSpecificEpithet());
      } catch (UnparsableException e) {
        LOG.warn("Fail to parse backbone name {}: {}", occ.getScientificName(), e);
      }

      for (Rank r : Rank.DWC_RANKS) {
        org.gbif.api.util.ClassificationUtils.setHigherRank(occ, r, match.getHigherRank(r));
        org.gbif.api.util.ClassificationUtils.setHigherRankKey(occ, r, match.getHigherRankKey(r));
      }
      LOG.debug("Got nub sci name [{}]", occ.getScientificName());
    }
  }
}
