/**
 *
 */
package org.gbif.occurrence.ws.factory;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.DwcTerm;

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
    // vo.setFields();
    Builder<String, String> b = ImmutableMap.<String, String>builder();
    putIfNotNull(b, DwcTerm.scientificName.simpleName(), from.getScientificName());
    putIfNotNull(b, DwcTerm.scientificNameAuthorship.simpleName(), from.getAuthor());
    putIfNotNull(b, DwcTerm.taxonRank.simpleName(), from.getRank());
    putIfNotNull(b, DwcTerm.kingdom.simpleName(), from.getKingdom());
    putIfNotNull(b, DwcTerm.phylum.simpleName(), from.getPhylum());
    putIfNotNull(b, DwcTerm.classs.simpleName(), from.getKlass());
    putIfNotNull(b, DwcTerm.order.simpleName(), from.getOrder());
    putIfNotNull(b, DwcTerm.family.simpleName(), from.getFamily());
    putIfNotNull(b, DwcTerm.genus.simpleName(), from.getGenus());
    putIfNotNull(b, DwcTerm.specificEpithet.simpleName(), from.getSpecies());
    putIfNotNull(b, DwcTerm.infraspecificEpithet.simpleName(), from.getSubspecies());
    putIfNotNull(b, DwcTerm.verbatimLatitude.simpleName(), from.getLatitude());
    putIfNotNull(b, DwcTerm.verbatimLongitude.simpleName(), from.getLongitude());
    // based on the belief that we most commonly mapped uncertaintity in the OLD portal
    putIfNotNull(b, DwcTerm.coordinateUncertaintyInMeters.simpleName(), from.getLatLongPrecision());
    putIfNotNull(b, DwcTerm.verbatimElevation.simpleName(),
      minMaxPrecision(from.getMinAltitude(), from.getMaxAltitude(), from.getAltitudePrecision()));
    putIfNotNull(b, DwcTerm.verbatimDepth.simpleName(), minMaxPrecision(from.getMinDepth(), from.getMaxDepth(), from.getDepthPrecision()));
    // DwC has expanded continentOrOcean into 2 terms, so they may be collapsed
    putIfNotNull(b, DwcTerm.continent.simpleName(), from.getContinentOrOcean(), "(originally supplied in continent OR ocean)");
    putIfNotNull(b, DwcTerm.country.simpleName(), from.getCountry());
    putIfNotNull(b, DwcTerm.stateProvince.simpleName(), from.getStateOrProvince());
    putIfNotNull(b, DwcTerm.county.simpleName(), from.getCounty());
    putIfNotNull(b, DwcTerm.recordedBy.simpleName(), from.getCollectorName());
    putIfNotNull(b, DwcTerm.locality.simpleName(), from.getLocality());
    putIfNotNull(b, DwcTerm.year.simpleName(), from.getYear());
    putIfNotNull(b, DwcTerm.month.simpleName(), from.getMonth());
    putIfNotNull(b, DwcTerm.day.simpleName(), from.getDay());
    putIfNotNull(b, DwcTerm.eventDate.simpleName(), from.getOccurrenceDate());
    putIfNotNull(b, DwcTerm.basisOfRecord.simpleName(), from.getBasisOfRecord());
    putIfNotNull(b, DwcTerm.identifiedBy.simpleName(), from.getIdentifierName());
    putIfNotNull(b, DwcTerm.dateIdentified.simpleName(),
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


  private static void putIfNotNull(Builder<String, String> b, String k, @Nullable String v) {
    if (v != null) {
      b.put(k, v);
    }
  }

  private static void putIfNotNull(Builder<String, String> b, String k, @Nullable String v, String suffix) {
    if (v != null) {
      putIfNotNull(b, k, v + " " + suffix);
    }
  }

  private static String yearMonthDayDate(String year, String month, String day, String date) {
    List<String> s = Lists.newArrayList();
    if (year != null) {
      s.add("Year[" + year + "]");
    }
    if (month != null) {
      s.add("Month[" + month + "]");
    }
    if (day != null) {
      s.add("Day[" + day + "]");
    }
    if (date != null) {
      s.add("Date[" + date + "]");
    }

    if (!s.isEmpty()) {
      return Joiner.on(" ").join(s);
    }
    return null;
  }

}
