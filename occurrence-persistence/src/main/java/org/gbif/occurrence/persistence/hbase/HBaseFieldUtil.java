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
    NAME_MAP.put(FieldName.DATASET_KEY, new HBaseColumn(cf, "dk"));
    NAME_MAP.put(FieldName.PUB_ORG_KEY, new HBaseColumn(cf, "pok"));
    NAME_MAP.put(FieldName.PUB_COUNTRY_CODE, new HBaseColumn(cf, "pcc"));
    NAME_MAP.put(FieldName.INSTITUTION_CODE, new HBaseColumn(cf, "ic"));
    NAME_MAP.put(FieldName.COLLECTION_CODE, new HBaseColumn(cf, "cc"));
    NAME_MAP.put(FieldName.CATALOG_NUMBER, new HBaseColumn(cf, "cn"));
    NAME_MAP.put(FieldName.IDENTIFIER_COUNT, new HBaseColumn(cf, "idc"));

    // parsing fragment
    NAME_MAP.put(FieldName.FRAGMENT, new HBaseColumn(cf, "x"));
    NAME_MAP.put(FieldName.FRAGMENT_HASH, new HBaseColumn(cf, "xh"));
    NAME_MAP.put(FieldName.XML_SCHEMA, new HBaseColumn(cf, "xs"));
    NAME_MAP.put(FieldName.LAST_CRAWLED, new HBaseColumn(cf, "lc"));
    NAME_MAP.put(FieldName.CRAWL_ID, new HBaseColumn(cf, "ci"));
    NAME_MAP.put(FieldName.PROTOCOL, new HBaseColumn(cf, "pr"));

    // verbatim occurrence
    NAME_MAP.put(FieldName.CREATED, new HBaseColumn(cf, "crtd"));
    NAME_MAP.put(FieldName.LAST_PARSED, new HBaseColumn(cf, "lp"));

    // interpreted occurrence
    NAME_MAP.put(FieldName.LAST_INTERPRETED, new HBaseColumn(cf, "li"));

    NAME_MAP.put(FieldName.I_KINGDOM, new HBaseColumn(cf, "ik"));
    NAME_MAP.put(FieldName.I_PHYLUM, new HBaseColumn(cf, "ip"));
    NAME_MAP.put(FieldName.I_CLASS, new HBaseColumn(cf, "icl"));
    NAME_MAP.put(FieldName.I_ORDER, new HBaseColumn(cf, "io"));
    NAME_MAP.put(FieldName.I_FAMILY, new HBaseColumn(cf, "if"));
    NAME_MAP.put(FieldName.I_GENUS, new HBaseColumn(cf, "ig"));
    NAME_MAP.put(FieldName.I_SUBGENUS, new HBaseColumn(cf, "isg"));
    NAME_MAP.put(FieldName.I_SPECIES, new HBaseColumn(cf, "is"));
    NAME_MAP.put(FieldName.I_SCIENTIFIC_NAME, new HBaseColumn(cf, "isn"));
    NAME_MAP.put(FieldName.I_SPECIFIC_EPITHET, new HBaseColumn(cf, "ise"));
    NAME_MAP.put(FieldName.I_INFRASPECIFIC_EPITHET, new HBaseColumn(cf, "iise"));
    NAME_MAP.put(FieldName.I_GENERIC_NAME, new HBaseColumn(cf, "ign"));
    NAME_MAP.put(FieldName.I_TAXON_RANK, new HBaseColumn(cf, "itr"));
    NAME_MAP.put(FieldName.I_KINGDOM_KEY, new HBaseColumn(cf, "ikk"));
    NAME_MAP.put(FieldName.I_PHYLUM_KEY, new HBaseColumn(cf, "ipk"));
    NAME_MAP.put(FieldName.I_CLASS_KEY, new HBaseColumn(cf, "ick"));
    NAME_MAP.put(FieldName.I_ORDER_KEY, new HBaseColumn(cf, "iok"));
    NAME_MAP.put(FieldName.I_FAMILY_KEY, new HBaseColumn(cf, "ifk"));
    NAME_MAP.put(FieldName.I_GENUS_KEY, new HBaseColumn(cf, "igk"));
    NAME_MAP.put(FieldName.I_SUBGENUS_KEY, new HBaseColumn(cf, "isgk"));
    NAME_MAP.put(FieldName.I_SPECIES_KEY, new HBaseColumn(cf, "isk"));
    NAME_MAP.put(FieldName.I_TAXON_KEY, new HBaseColumn(cf, "itk"));

    NAME_MAP.put(FieldName.I_CONTINENT, new HBaseColumn(cf, "icon"));
    NAME_MAP.put(FieldName.I_COUNTRY, new HBaseColumn(cf, "icc"));
    NAME_MAP.put(FieldName.I_STATE_PROVINCE, new HBaseColumn(cf, "isp"));
    NAME_MAP.put(FieldName.I_WATERBODY, new HBaseColumn(cf, "iwb"));

    NAME_MAP.put(FieldName.I_DECIMAL_LATITUDE, new HBaseColumn(cf, "ilat"));
    NAME_MAP.put(FieldName.I_DECIMAL_LONGITUDE, new HBaseColumn(cf, "ilng"));
    NAME_MAP.put(FieldName.I_COORD_ACCURACY, new HBaseColumn(cf, "icdacc"));
    NAME_MAP.put(FieldName.I_GEODETIC_DATUM, new HBaseColumn(cf, "igd"));
    NAME_MAP.put(FieldName.I_ELEVATION, new HBaseColumn(cf, "ialt"));
    NAME_MAP.put(FieldName.I_ELEVATION_ACC, new HBaseColumn(cf, "ialtacc"));
    NAME_MAP.put(FieldName.I_DEPTH, new HBaseColumn(cf, "idep"));
    NAME_MAP.put(FieldName.I_DEPTH_ACC, new HBaseColumn(cf, "idepacc"));
    NAME_MAP.put(FieldName.I_DIST_ABOVE_SURFACE, new HBaseColumn(cf, "idas"));
    NAME_MAP.put(FieldName.I_DIST_ABOVE_SURFACE_ACC, new HBaseColumn(cf, "idasacc"));

    NAME_MAP.put(FieldName.I_YEAR, new HBaseColumn(cf, "iy"));
    NAME_MAP.put(FieldName.I_MONTH, new HBaseColumn(cf, "im"));
    NAME_MAP.put(FieldName.I_DAY, new HBaseColumn(cf, "id"));
    NAME_MAP.put(FieldName.I_EVENT_DATE, new HBaseColumn(cf, "ied"));
    NAME_MAP.put(FieldName.I_DATE_IDENTIFIED, new HBaseColumn(cf, "idi"));

    NAME_MAP.put(FieldName.I_BASIS_OF_RECORD, new HBaseColumn(cf, "ibor"));
    NAME_MAP.put(FieldName.I_MODIFIED, new HBaseColumn(cf, "imod"));
    NAME_MAP.put(FieldName.SOURCE_MODIFIED, new HBaseColumn(cf, "sm"));
    NAME_MAP.put(FieldName.I_ESTAB_MEANS, new HBaseColumn(cf, "iem"));
    NAME_MAP.put(FieldName.I_INDIVIDUAL_COUNT, new HBaseColumn(cf, "iic"));
    NAME_MAP.put(FieldName.I_LIFE_STAGE, new HBaseColumn(cf, "ils"));
    NAME_MAP.put(FieldName.I_SEX, new HBaseColumn(cf, "isex"));
    NAME_MAP.put(FieldName.I_TYPE_STATUS, new HBaseColumn(cf, "its"));
    NAME_MAP.put(FieldName.I_TYPIFIED_NAME, new HBaseColumn(cf, "itn"));
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
