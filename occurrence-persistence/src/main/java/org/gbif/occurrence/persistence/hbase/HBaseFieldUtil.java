package org.gbif.occurrence.persistence.hbase;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.persistence.constants.HBaseTableConstants;

import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * Utility class to translate from FieldNames to their corresponding HBase column name (in the occurrence table). The
 * first value in the returned list is the field's column family, and the second value is the field's qualifier.
 */
public class HBaseFieldUtil {

  private static final Map<FieldName, HBaseColumn> NAME_MAP = Maps.newHashMap();

  static {
    // common fields
    String cf = HBaseTableConstants.OCCURRENCE_COLUMN_FAMILY;
    NAME_MAP.put(FieldName.DATA_PROVIDER_ID, new HBaseColumn(cf, "dpi"));
    NAME_MAP.put(FieldName.DATA_RESOURCE_ID, new HBaseColumn(cf, "dri"));
    NAME_MAP.put(FieldName.DATASET_KEY, new HBaseColumn(cf, "dk"));
    NAME_MAP.put(FieldName.OWNING_ORG_KEY, new HBaseColumn(cf, "ook"));
    NAME_MAP.put(FieldName.RESOURCE_ACCESS_POINT_ID, new HBaseColumn(cf, "rapi"));
    NAME_MAP.put(FieldName.INSTITUTION_CODE, new HBaseColumn(cf, "ic"));
    NAME_MAP.put(FieldName.COLLECTION_CODE, new HBaseColumn(cf, "cc"));
    NAME_MAP.put(FieldName.CATALOG_NUMBER, new HBaseColumn(cf, "cn"));
    NAME_MAP.put(FieldName.IDENTIFIER_COUNT, new HBaseColumn(cf, "idc"));

    // parsing fragment
    NAME_MAP.put(FieldName.FRAGMENT, new HBaseColumn(cf, "x"));
    NAME_MAP.put(FieldName.FRAGMENT_HASH, new HBaseColumn(cf, "xh"));
    NAME_MAP.put(FieldName.XML_SCHEMA, new HBaseColumn(cf, "xs"));
    NAME_MAP.put(FieldName.DWC_OCCURRENCE_ID, new HBaseColumn(cf, "doi"));
    NAME_MAP.put(FieldName.HARVESTED_DATE, new HBaseColumn(cf, "hd"));
    NAME_MAP.put(FieldName.CRAWL_ID, new HBaseColumn(cf, "ci"));
    NAME_MAP.put(FieldName.PROTOCOL, new HBaseColumn(cf, "pr"));

    // verbatim occurrence
    NAME_MAP.put(FieldName.SCIENTIFIC_NAME, new HBaseColumn(cf, "sn"));
    NAME_MAP.put(FieldName.AUTHOR, new HBaseColumn(cf, "a"));
    NAME_MAP.put(FieldName.RANK, new HBaseColumn(cf, "r"));
    NAME_MAP.put(FieldName.KINGDOM, new HBaseColumn(cf, "k"));
    NAME_MAP.put(FieldName.PHYLUM, new HBaseColumn(cf, "p"));
    NAME_MAP.put(FieldName.CLASS, new HBaseColumn(cf, "c"));
    NAME_MAP.put(FieldName.ORDER, new HBaseColumn(cf, "o"));
    NAME_MAP.put(FieldName.FAMILY, new HBaseColumn(cf, "f"));
    NAME_MAP.put(FieldName.GENUS, new HBaseColumn(cf, "g"));
    NAME_MAP.put(FieldName.SPECIES, new HBaseColumn(cf, "s"));
    NAME_MAP.put(FieldName.SUBSPECIES, new HBaseColumn(cf, "ss"));
    NAME_MAP.put(FieldName.LATITUDE, new HBaseColumn(cf, "lat"));
    NAME_MAP.put(FieldName.LONGITUDE, new HBaseColumn(cf, "lng"));
    NAME_MAP.put(FieldName.LAT_LNG_PRECISION, new HBaseColumn(cf, "llp"));
    NAME_MAP.put(FieldName.MAX_ALTITUDE, new HBaseColumn(cf, "maxa"));
    NAME_MAP.put(FieldName.MIN_ALTITUDE, new HBaseColumn(cf, "mina"));
    NAME_MAP.put(FieldName.ALTITUDE_PRECISION, new HBaseColumn(cf, "ap"));
    NAME_MAP.put(FieldName.MIN_DEPTH, new HBaseColumn(cf, "mind"));
    NAME_MAP.put(FieldName.MAX_DEPTH, new HBaseColumn(cf, "maxd"));
    NAME_MAP.put(FieldName.DEPTH_PRECISION, new HBaseColumn(cf, "dp"));
    NAME_MAP.put(FieldName.CONTINENT_OCEAN, new HBaseColumn(cf, "co"));
    NAME_MAP.put(FieldName.STATE_PROVINCE, new HBaseColumn(cf, "sp"));
    NAME_MAP.put(FieldName.COUNTY, new HBaseColumn(cf, "cty"));
    NAME_MAP.put(FieldName.COUNTRY, new HBaseColumn(cf, "ctry"));
    NAME_MAP.put(FieldName.COLLECTOR_NAME, new HBaseColumn(cf, "coln"));
    NAME_MAP.put(FieldName.LOCALITY, new HBaseColumn(cf, "loc"));
    NAME_MAP.put(FieldName.YEAR, new HBaseColumn(cf, "y"));
    NAME_MAP.put(FieldName.MONTH, new HBaseColumn(cf, "m"));
    NAME_MAP.put(FieldName.DAY, new HBaseColumn(cf, "d"));
    NAME_MAP.put(FieldName.OCCURRENCE_DATE, new HBaseColumn(cf, "od"));
    NAME_MAP.put(FieldName.BASIS_OF_RECORD, new HBaseColumn(cf, "bor"));
    NAME_MAP.put(FieldName.IDENTIFIER_NAME, new HBaseColumn(cf, "idn"));
    NAME_MAP.put(FieldName.IDENTIFICATION_DATE, new HBaseColumn(cf, "idd"));
    NAME_MAP.put(FieldName.UNIT_QUALIFIER, new HBaseColumn(cf, "uq"));
    NAME_MAP.put(FieldName.CREATED, new HBaseColumn(cf, "crtd"));
    NAME_MAP.put(FieldName.MODIFIED, new HBaseColumn(cf, "mod"));

    // interpreted occurrence
    NAME_MAP.put(FieldName.I_KINGDOM, new HBaseColumn(cf, "ik"));
    NAME_MAP.put(FieldName.I_PHYLUM, new HBaseColumn(cf, "ip"));
    NAME_MAP.put(FieldName.I_CLASS, new HBaseColumn(cf, "icl"));
    NAME_MAP.put(FieldName.I_ORDER, new HBaseColumn(cf, "io"));
    NAME_MAP.put(FieldName.I_FAMILY, new HBaseColumn(cf, "if"));
    NAME_MAP.put(FieldName.I_GENUS, new HBaseColumn(cf, "ig"));
    NAME_MAP.put(FieldName.I_SPECIES, new HBaseColumn(cf, "is"));
    NAME_MAP.put(FieldName.I_SCIENTIFIC_NAME, new HBaseColumn(cf, "isn"));
    NAME_MAP.put(FieldName.I_KINGDOM_ID, new HBaseColumn(cf, "iki"));
    NAME_MAP.put(FieldName.I_PHYLUM_ID, new HBaseColumn(cf, "ipi"));
    NAME_MAP.put(FieldName.I_CLASS_ID, new HBaseColumn(cf, "ici"));
    NAME_MAP.put(FieldName.I_ORDER_ID, new HBaseColumn(cf, "ioi"));
    NAME_MAP.put(FieldName.I_FAMILY_ID, new HBaseColumn(cf, "ifi"));
    NAME_MAP.put(FieldName.I_GENUS_ID, new HBaseColumn(cf, "igi"));
    NAME_MAP.put(FieldName.I_SPECIES_ID, new HBaseColumn(cf, "isi"));
    NAME_MAP.put(FieldName.I_NUB_ID, new HBaseColumn(cf, "ini"));
    NAME_MAP.put(FieldName.I_ISO_COUNTRY_CODE, new HBaseColumn(cf, "icc"));
    NAME_MAP.put(FieldName.I_LATITUDE, new HBaseColumn(cf, "ilat"));
    NAME_MAP.put(FieldName.I_LONGITUDE, new HBaseColumn(cf, "ilng"));
    NAME_MAP.put(FieldName.I_CELL_ID, new HBaseColumn(cf, "icell"));
    NAME_MAP.put(FieldName.I_CENTI_CELL_ID, new HBaseColumn(cf, "iccell"));
    NAME_MAP.put(FieldName.I_MOD360_CELL_ID, new HBaseColumn(cf, "i360"));
    NAME_MAP.put(FieldName.I_YEAR, new HBaseColumn(cf, "iy"));
    NAME_MAP.put(FieldName.I_MONTH, new HBaseColumn(cf, "im"));
    NAME_MAP.put(FieldName.I_OCCURRENCE_DATE, new HBaseColumn(cf, "iod"));
    NAME_MAP.put(FieldName.I_BASIS_OF_RECORD, new HBaseColumn(cf, "ibor"));
    NAME_MAP.put(FieldName.I_TAXONOMIC_ISSUE, new HBaseColumn(cf, "itaxi"));
    NAME_MAP.put(FieldName.I_GEOSPATIAL_ISSUE, new HBaseColumn(cf, "igeoi"));
    NAME_MAP.put(FieldName.I_OTHER_ISSUE, new HBaseColumn(cf, "iothi"));
    NAME_MAP.put(FieldName.I_ALTITUDE, new HBaseColumn(cf, "ialt"));
    NAME_MAP.put(FieldName.I_DEPTH, new HBaseColumn(cf, "idep"));
    NAME_MAP.put(FieldName.I_MODIFIED, new HBaseColumn(cf, "imod"));
    NAME_MAP.put(FieldName.PUBLISHING_COUNTRY, new HBaseColumn(cf, "hc"));
  }

  /**
   * Should never be instantiated.
   */
  private HBaseFieldUtil() {
  }

  public static HBaseColumn getHBaseColumn(@NotNull FieldName field) {
    checkNotNull(field, "field can't be null");

    return NAME_MAP.get(field);
  }

  @Nullable
  public static HBaseColumn getHBaseColumn(@NotNull Term term) {
    checkNotNull(term, "term can't be null");

    // unknown terms will never be mapped in Hive, and we can't replace : with anything and guarantee that it will
    // be reversible
    if (term instanceof UnknownTerm) {
      return new HBaseColumn(HBaseTableConstants.OCCURRENCE_COLUMN_FAMILY,
        HBaseTableConstants.UNKNOWN_TERM_PREFIX + term.toString());
    }

    // known terms are regularly mapped in Hive and Hive can't handle : in the hbase column name
    String safeTerm = term.toString().replaceAll(":", HBaseTableConstants.COLON_REPLACEMENT);
    return new HBaseColumn(HBaseTableConstants.OCCURRENCE_COLUMN_FAMILY,
      HBaseTableConstants.KNOWN_TERM_PREFIX + safeTerm);
  }

  @Nullable
  public static Term getTermFromColumn(@NotNull byte[] qualifier) {
    checkNotNull(qualifier, "qualifier can't be null");

    Term term = null;
    String colName = Bytes.toString(qualifier);
    if (colName.startsWith(HBaseTableConstants.KNOWN_TERM_PREFIX)) {
      String rawName = colName.substring(HBaseTableConstants.KNOWN_TERM_PREFIX.length());
      term = TermFactory.instance().findTerm(rawName.replaceAll(HBaseTableConstants.COLON_REPLACEMENT, ":"));
    } else if (colName.startsWith(HBaseTableConstants.UNKNOWN_TERM_PREFIX)) {
      term = TermFactory.instance().findTerm(colName.substring(HBaseTableConstants.UNKNOWN_TERM_PREFIX.length()));
    }

    return term;
  }

  public static HBaseColumn getHBaseColumn(@NotNull OccurrenceIssue issue) {
    checkNotNull(issue, "issue can't be null");

    return new HBaseColumn(HBaseTableConstants.OCCURRENCE_COLUMN_FAMILY,
      HBaseTableConstants.ISSUE_PREFIX + issue.name());
  }

  public static class HBaseColumn {

    private final String columnFamilyName;
    private final String columnName;

    public HBaseColumn(String columnFamilyName, String columnName) {
      this.columnFamilyName = columnFamilyName;
      this.columnName = columnName;
    }

    public String getColumnFamilyName() {
      return columnFamilyName;
    }

    public String getColumnName() {
      return columnName;
    }
  }
}
