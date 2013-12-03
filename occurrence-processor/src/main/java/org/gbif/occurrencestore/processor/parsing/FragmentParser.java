package org.gbif.occurrencestore.processor.parsing;

import org.gbif.occurrence.model.RawOccurrenceRecord;
import org.gbif.occurrence.parsing.xml.XmlFragmentParser;
import org.gbif.occurrencestore.persistence.api.Fragment;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrence;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * A thin wrapper around the XmlFragmentParser from the occurrence-parser project, and the local JsonFragmentParser, in
 * order to produce VerbatimOccurrence objects from Fragments.
 */
public class FragmentParser {

  private static final Timer xmlParsingTimer =
    Metrics.newTimer(FragmentParser.class, "verb xml parse time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private static final Timer jsonParsingTimer =
    Metrics.newTimer(FragmentParser.class, "verb json parse time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  // should not be instantiated
  private FragmentParser() {
  }

  /**
   * Parse the given fragment into a VerbatimOccurrence object.
   * TODO: there is a lot of hacking here that should be refactored out with general rework of parsing project
   *
   * @param fragment containing parsing to be parsed
   *
   * @return a VerbatimOccurrence or null if the fragment could not be parsed
   */
  @Nullable
  public static VerbatimOccurrence parse(Fragment fragment) {
    VerbatimOccurrence verbatim = null;

    switch (fragment.getFragmentType()) {
      case XML:
        final TimerContext xmlContext = xmlParsingTimer.time();
        try {
          RawOccurrenceRecord ror =
            XmlFragmentParser.parseRecord(fragment.getData(), fragment.getXmlSchema(), fragment.getUnitQualifier());
          verbatim = buildVerbatim(ror, fragment);
        } finally {
          xmlContext.stop();
        }
        break;
      case JSON:
        final TimerContext jsonContext = jsonParsingTimer.time();
        try {
          verbatim = JsonFragmentParser.parseRecord(fragment);
        } finally {
          jsonContext.stop();
        }
        break;
    }

    if (verbatim != null) {
      verbatim.setProtocol(fragment.getProtocol());
    }

    return verbatim;
  }

  // TODO: identifier records
  private static VerbatimOccurrence buildVerbatim(RawOccurrenceRecord ror, Fragment frag) {
    VerbatimOccurrence v = VerbatimOccurrence.builder().key(frag.getKey()).datasetKey(frag.getDatasetKey())
      .altitudePrecision(ror.getAltitudePrecision()).author(ror.getAuthor()).basisOfRecord(ror.getBasisOfRecord())
      .catalogNumber(ror.getCatalogueNumber()).collectionCode(ror.getCollectionCode())
      .collectorName(ror.getCollectorName()).continentOrOcean(ror.getContinentOrOcean()).country(ror.getCountry())
      .county(ror.getCounty()).dataProviderId(ror.getDataProviderId()).dataResourceId(ror.getDataResourceId())
      .dateIdentified(ror.getDateIdentified()).day(ror.getDay()).dayIdentified(ror.getDayIdentified())
      .dateIdentified(ror.getDateIdentified()).depthPrecision(ror.getDepthPrecision()).family(ror.getFamily())
      .genus(ror.getGenus()).identifierName(ror.getIdentifierName()).institutionCode(ror.getInstitutionCode())
      .kingdom(ror.getKingdom()).klass(ror.getKlass()).latitude(ror.getLatitude()).longitude(ror.getLongitude())
      .latLongPrecision(ror.getLatLongPrecision()).locality(ror.getLocality()).maxAltitude(ror.getMaxAltitude())
      .maxDepth(ror.getMaxDepth()).minAltitude(ror.getMinAltitude()).minDepth(ror.getMinDepth())
      .modified(ror.getModified()).month(ror.getMonth()).monthIdentified(ror.getMonthIdentified())
      .occurrenceDate(ror.getOccurrenceDate()).order(ror.getOrder()).phylum(ror.getPhylum()).rank(ror.getRank())
      .resourceAccessPointId(ror.getResourceAccessPointId()).scientificName(ror.getScientificName())
      .species(ror.getSpecies()).stateOrProvince(ror.getStateOrProvince()).subspecies(ror.getSubspecies())
      .unitQualifier(ror.getUnitQualifier()).year(ror.getYear()).yearIdentified(ror.getYearIdentified()).build();

    return v;
  }
}
