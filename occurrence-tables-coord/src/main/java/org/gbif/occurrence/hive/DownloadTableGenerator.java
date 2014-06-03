package org.gbif.occurrence.hive;

import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.hive.udf.ArrayNullsRemoverGenericUDF;
import org.gbif.occurrence.hive.udf.CollectMediaTypesUDF;
import org.gbif.occurrence.persistence.hbase.Columns;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

/**
 * Utility class that generates the Hive scripts required to create an HDFS table that is used by occurrence downloads.
 */
public class DownloadTableGenerator {

  private static final String VERBATIM_COL_FMT = "v_%s";
  private static final String VERBATIM_COL_DECL_FMT = VERBATIM_COL_FMT + " STRING";
  private static final String STRING_COL_DECL_FMT = "%s STRING";
  private static final Joiner COMMA_JOINER = Joiner.on(',').skipNulls();
  protected static final String COLLECT_MEDIATYPES_UDF_DCL =
    "CREATE TEMPORARY FUNCTION collectMediaTypes AS '" + CollectMediaTypesUDF.class.getName() + "'";
  protected static final String REMOVE_NULLS_UDF_DCL =
    "CREATE TEMPORARY FUNCTION removeNulls AS '" + ArrayNullsRemoverGenericUDF.class.getName() + "'";
  private static final String COLLECT_MEDIATYPES = "collectMediaTypes("
    + HiveColumnsUtils.getHiveColumn(Extension.MULTIMEDIA) + ")";
  private static final String HIVE_CREATE_HBASE_TABLE_FMT =
    "CREATE EXTERNAL TABLE IF NOT EXISTS %s (%s) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \"%s\") TBLPROPERTIES(\"hbase.table.name\" = \"%s\",\"hbase.table.default.storage.type\" = \"binary\")";

  private static final String HIVE_CREATE_HDFS_TABLE_FMT = "CREATE TABLE IF NOT EXISTS %s (%s) STORED AS RCFILE";

  private static final String DROP_TABLE_FMT = "DROP TABLE IF EXISTS %s";

  protected static final String HDFS_POST = "_hdfs";
  protected static final String HBASE_POST = "_hbase";

  private static final String HBASE_MAP_FMT = "o:%s";
  private static final String HBASE_KEY_MAPPING = ":key";
  private static final String OCC_ID_COL_DEF = HiveColumnsUtils.getHiveColumn(GbifTerm.gbifID) + " INT";
  protected static final String HIVE_DEFAULT_OPTS =
    "SET hive.exec.compress.output=true;SET mapred.max.split.size=256000000;SET mapred.output.compression.type=BLOCK;SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;SET hive.hadoop.supports.splittable.combineinputformat=true;SET hbase.client.scanner.caching=200;SET hive.mapred.reduce.tasks.speculative.execution=false;SET hive.mapred.map.tasks.speculative.execution=false";
  private static final String INSERT_INFO_OCCURRENCE_HDFS =
    "INSERT OVERWRITE TABLE %1$s SELECT %2$s FROM %3$s";

  private static final String HIVE_CMDS_SEP = ";\n";
  private static final Joiner HIVE_CMDS_JOINER = Joiner.on(HIVE_CMDS_SEP).skipNulls();

  private static final String HAS_COORDINATE_EXP = "(decimalLongitude IS NOT NULL AND decimalLatitude IS NOT NULL)";
  private static final String ISSUE_ENTRY_EXP = "CASE COALESCE(%1$s,0) WHEN 0 THEN NULL ELSE %2$s END";

  private static final String ISSUE_HIVE_TYPE = " INT";
  private static final String COALESCE0_FMT = "COALESCE(%s,0)";

  private static final EnumSet<GbifInternalTerm> INTERNAL_TERMS = EnumSet.complementOf(EnumSet.of(
    GbifInternalTerm.fragmentHash,
    GbifInternalTerm.fragment));

  /**
   * Generates the COLUMN DATATYPE declaration.
   */
  private static final Function<Term, String> TERM_COL_DECL = new Function<Term, String>() {

    public String apply(Term term) {
      return HiveColumnsUtils.getHiveColumn(term) + " " + HiveColumnsUtils.getHiveType(term);
    }
  };

  /**
   * Generates the COLUMN name of a verbatim term.
   */
  private static final Function<Term, String> VERBATIM_TERM_COL_DEF = new Function<Term, String>() {

    public String apply(Term term) {
      return String.format(VERBATIM_COL_FMT, HiveColumnsUtils.getHiveColumn(term));
    }
  };


  /**
   * Generates the COLUMN name of a extension.
   */
  private static final Function<Extension, String> EXTENSION_COL_DECL = new Function<Extension, String>() {

    public String apply(Extension extension) {
      return String.format(STRING_COL_DECL_FMT, HiveColumnsUtils.getHiveColumn(extension));
    }
  };


  /**
   * Generates the COLUMN declaration (NAME TYPE) of a verbatim term.
   */
  private static final Function<Term, String> VERBATIM_TERM_COL_DECL = new Function<Term, String>() {

    public String apply(Term term) {
      return String.format(VERBATIM_COL_DECL_FMT, HiveColumnsUtils.getHiveColumn(term));
    }
  };


  /**
   * Generates "COALESCE" expression: COALESCE(0,issue column name).
   */
  private static final Function<OccurrenceIssue, String> ISSUE_COALESCE_EXP = new Function<OccurrenceIssue, String>() {

    public String apply(OccurrenceIssue issue) {
      return String.format(COALESCE0_FMT, HiveColumnsUtils.getHiveColumn(issue));
    }
  };

  /**
   * Generates "COALESCE" expression: COALESCE(0,issue column name).
   */
  private static final Function<OccurrenceIssue, String> ISSUES_ARRAY_EXP = new Function<OccurrenceIssue, String>() {

    public String apply(OccurrenceIssue issue) {
      return String.format(ISSUE_ENTRY_EXP, HiveColumnsUtils.getHiveColumn(issue), "'" + issue.name() + "'");
    }
  };

  /**
   * Generates the select expression of column.
   */
  private static final Function<Term, String> TERM_SELECT_EXP = new Function<Term, String>() {

    public String apply(Term term) {
      if (GbifTerm.hasGeospatialIssues == term) {
        return hasSpatialIssueQuery();
      } else if (GbifTerm.hasCoordinate == term) {
        return HAS_COORDINATE_EXP;
      } else if (GbifTerm.issue == term) {
        return issuesArrayQuery();
      }
      return HiveColumnsUtils.getHiveColumn(term);
    }

  };

  /**
   * Generates a Hive expression that validates if a record has spatial issues.
   */
  private static String hasSpatialIssueQuery() {
    return "("
      + Joiner.on(" + ").skipNulls().join(Lists.transform(OccurrenceIssue.GEOSPATIAL_RULES, ISSUE_COALESCE_EXP))
      + ") > 0";
  }

  /**
   * Generates a Hive expression that validates if a record has spatial issues.
   */
  private static String issuesArrayQuery() {
    final List<OccurrenceIssue> issuesList = Arrays.asList(OccurrenceIssue.values());
    return "removeNulls(array("
      + Joiner.on(',').skipNulls().join(Lists.transform(issuesList, ISSUES_ARRAY_EXP))
      + "))";
  }

  /**
   * Generates the HBaseMapping of a interpreted or internal Term.
   */
  private static final Function<Term, String> INT_TERM_MAPPING_DEF = new Function<Term, String>() {

    public String apply(Term term) {
      return String.format(HBASE_MAP_FMT, Columns.column(term));
    }
  };


  /**
   * Generates the HBaseMapping of a verbatim Term.
   */
  private static final Function<Term, String> VERB_TERM_MAPPING_DEF = new Function<Term, String>() {

    public String apply(Term term) {
      return String.format(HBASE_MAP_FMT, Columns.verbatimColumn(term));
    }
  };

  /**
   * Generates the HBaseMapping of interpreted Term.
   */
  private static final Function<OccurrenceIssue, String> ISSUE_MAPPING_DEF = new Function<OccurrenceIssue, String>() {

    public String apply(OccurrenceIssue issue) {
      return String.format(HBASE_MAP_FMT, Columns.column(issue));
    }
  };

  /**
   * Generates the column declaration (NAME TYPE) of occurrence issue.
   */
  private static final Function<OccurrenceIssue, String> ISSUE_COL_DECL = new Function<OccurrenceIssue, String>() {

    public String apply(OccurrenceIssue issue) {
      return HiveColumnsUtils.getHiveColumn(issue) + ISSUE_HIVE_TYPE;
    }
  };


  /**
   * Utility function that applies a function to a iterable list of terms excluding the terms listed in the exclusions
   * parameters.
   */
  private static Iterable<String> processTerms(Iterable<? extends Term> terms, Function<Term, String> transformer,
    Set<Term> exclusions) {
    return Iterables.transform(Iterables.filter(terms, Predicates.not(Predicates.in(exclusions))), transformer);
  }

  /**
   * Utility function that applies a function to a iterable list of occurrence issues excluding the terms listed in the
   * exclusions parameters.
   */
  private static Iterable<String> processIssues(Function<OccurrenceIssue, String> transformer) {
    return Iterables.transform(Arrays.asList(OccurrenceIssue.values()), transformer);
  }


  /**
   * Returns a list of column names for the HDFS table.
   */
  protected static List<String> hdfsTableCommonColumns() {
    ImmutableSet<Term> exclusions =
      new ImmutableSet.Builder<Term>().add(GbifTerm.gbifID).add(GbifTerm.mediaType).build();
    List<String> columns =
      new ImmutableList.Builder<String>()
        .add(OCC_ID_COL_DEF)
        .addAll(processTerms(TermUtils.verbatimTerms(), VERBATIM_TERM_COL_DECL, exclusions))
        .addAll(processTerms(INTERNAL_TERMS, TERM_COL_DECL, exclusions))
        .addAll(processTerms(TermUtils.interpretedTerms(), TERM_COL_DECL, exclusions))
        .build();
    return columns;
  }

  /**
   * Lists the Hive select expressions to populate the HDFS table from the Hbase table.
   */
  private static List<String> selectHdfsTableColumns() {
    ImmutableSet<Term> exclusions =
      new ImmutableSet.Builder<Term>().add(GbifTerm.gbifID).add(GbifTerm.mediaType).build();
    List<String> columns =
      new ImmutableList.Builder<String>()
        .add(HiveColumnsUtils.getHiveColumn(GbifTerm.gbifID))
        .addAll(processTerms(TermUtils.verbatimTerms(), VERBATIM_TERM_COL_DEF, exclusions))
        .addAll(processTerms(INTERNAL_TERMS, TERM_SELECT_EXP, exclusions))
        .addAll(processTerms(TermUtils.interpretedTerms(), TERM_SELECT_EXP, exclusions))
        .add(COLLECT_MEDIATYPES)
        .add(HiveColumnsUtils.getHiveColumn(Extension.MULTIMEDIA))
        .build();
    return columns;
  }

  /**
   * List the hive columns of the Hbase table.
   */
  private static List<String> hbaseTableColumns() {
    List<String> columns =
      new ImmutableList.Builder<String>()
        .addAll(hdfsTableCommonColumns())
        .addAll(processIssues(ISSUE_COL_DECL))
        .add(EXTENSION_COL_DECL.apply(Extension.MULTIMEDIA)).build();
    return columns;
  }

  /**
   * List the Hive-to-HBase-Hive column mappings.
   */
  private static List<String> hbaseTableColumnMappings() {
    ImmutableSet<Term> exclusions =
      new ImmutableSet.Builder<Term>().add(GbifTerm.gbifID).add(GbifTerm.mediaType).build();
    List<String> columns =
      new ImmutableList.Builder<String>()
        .add(HBASE_KEY_MAPPING)
        .addAll(processTerms(TermUtils.verbatimTerms(), VERB_TERM_MAPPING_DEF, exclusions))
        .addAll(processTerms(INTERNAL_TERMS, INT_TERM_MAPPING_DEF, exclusions))
        .addAll(processTerms(TermUtils.interpretedTerms(), INT_TERM_MAPPING_DEF, exclusions))
        .addAll(processIssues(ISSUE_MAPPING_DEF))
        .add(String.format(HBASE_MAP_FMT, Columns.column(Extension.MULTIMEDIA))).build();
    return columns;
  }

  /**
   * Builds the CREATE TABLE statement for the HDFS table.
   */
  protected static String buildCreateHdfsTable(String hiveTableName) {
    return String.format(HIVE_CREATE_HDFS_TABLE_FMT, hiveTableName + HDFS_POST, COMMA_JOINER.join(hdfsTableColumns()));
  }

  protected static List<String> hdfsTableColumns() {
    return new ImmutableList.Builder<String>().addAll(hdfsTableCommonColumns())
      .add(TERM_COL_DECL.apply(GbifTerm.mediaType))
      .add(EXTENSION_COL_DECL.apply(Extension.MULTIMEDIA)).build();
  }


  /**
   * Builds the CREATE TABLE statement for the HBase table.
   */
  protected static String buildCreateHBaseTable(String hiveTableName, String hbaseTableName) {
    return String.format(HIVE_CREATE_HBASE_TABLE_FMT, hiveTableName + HBASE_POST,
      COMMA_JOINER.join(hbaseTableColumns()),
      COMMA_JOINER.join(hbaseTableColumnMappings()), hbaseTableName);
  }

  /**
   * Builds the INSERT OVERWRITE statement that populates the HDFS table from HBase.
   */
  protected static String buildInsertFromHBaseIntoHive(String hiveTableName) {
    return String.format(INSERT_INFO_OCCURRENCE_HDFS, hiveTableName + HDFS_POST,
      COMMA_JOINER.join(selectHdfsTableColumns()), hiveTableName + HBASE_POST);
  }

  /**
   * Generates the drop table statements for the hdfs and hbase backed tables.
   */
  protected static String buildDropTableStatements(String hiveTableName) {
    return String.format(DROP_TABLE_FMT, hiveTableName + HDFS_POST) + ';'
      + String.format(DROP_TABLE_FMT, hiveTableName + HBASE_POST);
  }

  /**
   * Generates a Hive script that deletes the hive occurrence tables, creates them and populate the HDFS table.
   */
  protected static String generateHiveScript(String hiveTableName, String hbaseTableName, boolean dropTablesFirst) {
    return HIVE_CMDS_JOINER
      .join(HIVE_DEFAULT_OPTS, COLLECT_MEDIATYPES_UDF_DCL, REMOVE_NULLS_UDF_DCL,
        (dropTablesFirst ? buildDropTableStatements(hiveTableName) : null),
        buildCreateHdfsTable(hiveTableName), buildCreateHBaseTable(hiveTableName, hbaseTableName),
        buildInsertFromHBaseIntoHive(hiveTableName))
      + HIVE_CMDS_SEP;
  }


  /**
   * Entry point.
   * Requires 2 arguments: the Hive table name and the HBase table name.
   * The hive table name, preferably, shouldn't contain 'hdfs' at the end because the script by default add it.
   * Optional argument: output file, if the script is generated into a file.
   */
  public static void main(String[] args) throws Exception {
    Closer closer = Closer.create();
    if (args.length < 2) {
      throw new IllegalArgumentException(
        "At least 2 parameters are required: the hive output table and the hbase input table");
    }
    final String hiveTableName = args[0];
    final String hbaseTableName = args[1];
    if (args.length > 2) {
      System.setOut(closer.register(new PrintStream(new File(args[3]))));
    } else {
      System.out.println("No output file parameter specified, using the console as output");
    }
    System.out.println(DownloadTableGenerator.generateHiveScript(hiveTableName, hbaseTableName, true));
    closer.close();
  }
}
