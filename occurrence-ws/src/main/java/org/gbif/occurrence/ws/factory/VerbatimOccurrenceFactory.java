/**
 *
 */
package org.gbif.occurrence.ws.factory;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

import java.util.List;
import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;

/**
 * This is responsible for transforming the verbatim occurrence persistence object
 * into the API representation which is Map based. All map entries are keyed using
 * the DwC simple term names. Transposition of terms that don't map is implemented for
 * <ul>
 * <li>MinAltitude, MaxAltitude, Altitude Precision -> verbatimElevation</li>
 * <li>MinDepth, MaxDepth, Depth Precision -> verbatimDepth</li>
 * <li>yearIdentified, monthIdentified, dayIdentified, dateIdentified -> dateIdentified</li>
 * </ul>
 */
public class VerbatimOccurrenceFactory {

  public static VerbatimOccurrence build(org.gbif.occurrencestore.persistence.api.VerbatimOccurrence from) {
    VerbatimOccurrence vo = new VerbatimOccurrence();
    vo.setKey(from.getKey());
    Builder<Term, String> b = ImmutableMap.builder();
    putIfNotNull(b, DwcTerm.scientificName, from.getScientificName());
    putIfNotNull(b, DwcTerm.scientificNameAuthorship, from.getAuthor());
    putIfNotNull(b, DwcTerm.taxonRank, from.getRank());
    putIfNotNull(b, DwcTerm.kingdom, from.getKingdom());
    putIfNotNull(b, DwcTerm.phylum, from.getPhylum());
    putIfNotNull(b, DwcTerm.class_, from.getKlass());
    putIfNotNull(b, DwcTerm.order, from.getOrder());
    putIfNotNull(b, DwcTerm.family, from.getFamily());
    putIfNotNull(b, DwcTerm.genus, from.getGenus());
    putIfNotNull(b, DwcTerm.specificEpithet, from.getSpecies());
    putIfNotNull(b, DwcTerm.infraspecificEpithet, from.getSubspecies());
    putIfNotNull(b, DwcTerm.verbatimLatitude, from.getLatitude());
    putIfNotNull(b, DwcTerm.verbatimLongitude, from.getLongitude());
    // based on the belief that we most commonly mapped uncertaintity in the OLD portal
    putIfNotNull(b, DwcTerm.coordinateUncertaintyInMeters, from.getLatLongPrecision());
    putIfNotNull(b, DwcTerm.verbatimElevation,
      minMaxPrecision(from.getMinAltitude(), from.getMaxAltitude(), from.getAltitudePrecision()));
    putIfNotNull(b, DwcTerm.verbatimDepth, minMaxPrecision(from.getMinDepth(), from.getMaxDepth(), from.getDepthPrecision()));
    // DwC has expanded continentOrOcean into 2 terms, so they may be collapsed
    putIfNotNull(b, DwcTerm.continent, from.getContinentOrOcean(), "(originally supplied in continent OR ocean)");
    putIfNotNull(b, DwcTerm.country, from.getCountry());
    putIfNotNull(b, DwcTerm.stateProvince, from.getStateOrProvince());
    putIfNotNull(b, DwcTerm.county, from.getCounty());
    putIfNotNull(b, DwcTerm.recordedBy, from.getCollectorName());
    putIfNotNull(b, DwcTerm.locality, from.getLocality());
    putIfNotNull(b, DwcTerm.year, from.getYear());
    putIfNotNull(b, DwcTerm.month, from.getMonth());
    putIfNotNull(b, DwcTerm.day, from.getDay());
    putIfNotNull(b, DwcTerm.eventDate, from.getOccurrenceDate());
    putIfNotNull(b, DwcTerm.basisOfRecord, from.getBasisOfRecord());
    putIfNotNull(b, DwcTerm.identifiedBy, from.getIdentifierName());
    putIfNotNull(b, DwcTerm.dateIdentified,
      yearMonthDayDate(from.getYearIdentified(), from.getMonthIdentified(), from.getDayIdentified(), from.getDateIdentified()));
    vo.setFields(b.build());
    return vo;
  }

  private static String minMaxPrecision(String min, String max, String precision) {
    List<String> s = Lists.newArrayList();
    if (min != null) {
      s.add("Min[" + min + "]");
    }
    if (max != null) {
      s.add("Max[" + max + "]");
    }
    if (precision != null) {
      s.add("Precision[" + precision + "]");
    }

    if (!s.isEmpty()) {
      return Joiner.on(" ").join(s);
    }
    return null;
  }


  private static void putIfNotNull(Builder<Term, String> b, Term k, @Nullable String v) {
    if (v != null) {
      b.put(k, v);
    }
  }

  private static void putIfNotNull(Builder<Term, String> b, Term k, @Nullable String v, String suffix) {
    if (v != null) {
      putIfNotNull(b, k, v + ' ' + suffix);
    }
  }

  private static String yearMonthDayDate(String year, String month, String day, String date) {
    List<String> s = Lists.newArrayList();
    if (year != null) {
      s.add("Year[" + year + ']');
    }
    if (month != null) {
      s.add("Month[" + month + ']');
    }
    if (day != null) {
      s.add("Day[" + day + ']');
    }
    if (date != null) {
      s.add("Date[" + date + ']');
    }

    if (!s.isEmpty()) {
      return Joiner.on(" ").join(s);
    }
    return null;
  }

}
