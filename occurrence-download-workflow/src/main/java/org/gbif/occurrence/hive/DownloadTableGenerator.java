package org.gbif.occurrence.hive;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.persistence.hbase.Columns;

import java.io.File;
import java.io.IOException;
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
  private static final Joiner COMMA_JOINER = Joiner.on(',').skipNulls();
  private static final String HIVE_CREATE_HBASE_TABLE_FMT =
    "CREATE EXTERNAL TABLE %s (%s) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \"%s\") TBLPROPERTIES(\"hbase.table.name\" = \"%s\",\"hbase.table.default.storage.type\" = \"binary\");";

  private static final String HIVE_CREATE_HDFS_TABLE_FMT = "CREATE TABLE %s (%s) STORED AS RCFILE;";


  private static final String MULTIMEDIA_TABLE_COLUMNS =
    "gbifid INT,type STRING,format_ STRING,identifier STRING,references STRING,title STRING,description STRING,source STRING,audience STRING,created BIGINT,creator STRING,contributor STRING, publisher STRING,license STRING,rightsHolder STRING";
  private static final String MULTIMEDIA_TABLE_NAME = "multimedia_hdfs";
  private static final String HIVE_MULTIMEDIA_TABLE = String.format(HIVE_CREATE_HDFS_TABLE_FMT, MULTIMEDIA_TABLE_NAME,
    MULTIMEDIA_TABLE_COLUMNS);


  private static final String DROP_TABLE_FMT = "DROP TABLE IF EXISTS %s;";

  private static final String HBASE_MAP_FMT = "o:%s";
  private static final String HBASE_KEY_MAPPING = ":key";
  private static final String OCC_ID_COL_DEF = TermUtils.getHiveColumn(GbifTerm.gbifID) + " INT";
  private static final String HIVE_DEFAULT_OPTS =
    "SET hive.exec.compress.output=true;SET mapred.max.split.size=256000000;SET mapred.output.compression.type=BLOCK;SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;SET hive.hadoop.supports.splittable.combineinputformat=true;SET hbase.client.scanner.caching=200;SET hive.mapred.reduce.tasks.speculative.execution=false;SET hive.mapred.map.tasks.speculative.execution=false;";
  private static final String INSERT_INFO_OCCURRENCE_HDFS = "INSERT OVERWRITE TABLE %1$s SELECT %2$s FROM %3$s;";


  private static final String INSERT_MULTIMEDIA_FMT =
    "INSERT OVERWRITE TABLE "
      + MULTIMEDIA_TABLE_NAME
      + " SELECT gbifid, mm_record['type'],mm_record['format'],mm_record['identifier'],mm_record['references'],mm_record['title'],mm_record['description'],mm_record['source'],mm_record['audience'],mm_record['created'],mm_record['creator'],mm_record['contributor'],mm_record['publisher'],mm_record['license'],mm_record['rightsHolder'] FROM %s lateral view explode(from_json(multimedia, 'array<map<string,string>>')) x AS mm_record;";

  private static final String HAS_COORDINATE_EXP = "(decimalLongitude IS NOT NULL AND decimalLatitude IS NOT NULL)";

  private static final String HDFS_POST = "_hdfs";
  private static final String HBASE_POST = "_hbase";

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
      return TermUtils.getHiveColumn(term) + " " + TermUtils.getHiveType(term);
    }
  };

  /**
   * Generates the COLUMN name of a verbatim term.
   */
  private static final Function<Term, String> VERBATIM_TERM_COL_DEF = new Function<Term, String>() {

    public String apply(Term term) {
      return String.format(VERBATIM_COL_FMT, TermUtils.getHiveColumn(term));
    }
  };

  /**
   * Generates the COLUMN declaration (NAME TYPE) of a verbatim term.
   */
  private static final Function<Term, String> VERBATIM_TERM_COL_DECL = new Function<Term, String>() {

    public String apply(Term term) {
      return String.format(VERBATIM_COL_DECL_FMT, TermUtils.getHiveColumn(term));
    }
  };


  /**
   * Generates "COALESCE" expression: COALESCE(0,issue column name).
   */
  private static final Function<OccurrenceIssue, String> ISSUE_COALESCE_EXP = new Function<OccurrenceIssue, String>() {

    public String apply(OccurrenceIssue issue) {
      return String.format(COALESCE0_FMT, TermUtils.getHiveColumn(issue));
    }
  };

  /**
   * Generates the select expression of column.
   */
  private static final Function<Term, String> TERM_SELECT_EXP = new Function<Term, String>() {

    public String apply(Term term) {
      if (GbifTerm.hasGeospatialIssues == term) {
        return hasSpatialIssueQuery();
      }
      if (GbifTerm.hasCoordinate == term) {
        return HAS_COORDINATE_EXP;
      }
      return TermUtils.getHiveColumn(term);
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
      return TermUtils.getHiveColumn(issue) + ISSUE_HIVE_TYPE;
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
  private static List<String> hdfsTableColumns() {
    ImmutableSet<Term> exclusions = new ImmutableSet.Builder<Term>().add(GbifTerm.gbifID).build();
    List<String> columns =
      new ImmutableList.Builder<String>()
        .add(OCC_ID_COL_DEF)
        .addAll(processTerms(TermUtils.verbatimTerms(), VERBATIM_TERM_COL_DECL, exclusions))
        .addAll(processTerms(INTERNAL_TERMS, TERM_COL_DECL, exclusions))
        .addAll(processTerms(TermUtils.interpretedTerms(), TERM_COL_DECL, exclusions)).build();
    return columns;
  }


  /**
   * Lists the Hive select expressions to populate the HDFS table from the Hbase table.
   */
  private static List<String> selectHdfsTableColumns() {
    ImmutableSet<Term> exclusions = new ImmutableSet.Builder<Term>().add(GbifTerm.gbifID).build();
    List<String> columns =
      new ImmutableList.Builder<String>()
        .add(TermUtils.getHiveColumn(GbifTerm.gbifID))
        .addAll(processTerms(TermUtils.verbatimTerms(), VERBATIM_TERM_COL_DEF, exclusions))
        .addAll(processTerms(INTERNAL_TERMS, TERM_SELECT_EXP, exclusions))
        .addAll(processTerms(TermUtils.interpretedTerms(), TERM_SELECT_EXP, exclusions)).build();
    return columns;
  }

  /**
   * List the hive columns of the Hbase table.
   */
  private static List<String> hbaseTableColumns() {
    List<String> columns =
      new ImmutableList.Builder<String>()
        .addAll(hdfsTableColumns())
        .addAll(processIssues(ISSUE_COL_DECL)).build();
    return columns;
  }

  /**
   * List the Hive-to-HBase-Hive column mappings.
   */
  private static List<String> hbaseTableColumnMappings() {
    ImmutableSet<Term> exclusions = new ImmutableSet.Builder<Term>().add(GbifTerm.gbifID).build();
    List<String> columns =
      new ImmutableList.Builder<String>()
        .add(HBASE_KEY_MAPPING)
        .addAll(processTerms(TermUtils.verbatimTerms(), VERB_TERM_MAPPING_DEF, exclusions))
        .addAll(processTerms(INTERNAL_TERMS, INT_TERM_MAPPING_DEF, exclusions))
        .addAll(processTerms(TermUtils.interpretedTerms(), INT_TERM_MAPPING_DEF, exclusions))
        .addAll(processIssues(ISSUE_MAPPING_DEF)).build();
    return columns;
  }

  /**
   * Builds the CREATE TABLE statement for the HDFS table.
   */
  private static String buildCreateHdfsTable(String hiveTableName) {
    return String.format(HIVE_CREATE_HDFS_TABLE_FMT, hiveTableName + HDFS_POST, COMMA_JOINER.join(hdfsTableColumns()));
  }

  /**
   * Builds the CREATE TABLE statement for the HBase table.
   */
  private static String buildCreateHBaseTable(String hiveTableName, String hbaseTableName) {
    return String.format(HIVE_CREATE_HBASE_TABLE_FMT, hiveTableName + HBASE_POST,
      COMMA_JOINER.join(hbaseTableColumns()),
      COMMA_JOINER.join(hbaseTableColumnMappings()), hbaseTableName);
  }

  /**
   * Builds the INSERT OVERWRITE statement that populates the HDFS table from HBase.
   */
  private static String buildInsertFromHBaseIntoHive(String hiveTableName) {
    return String.format(INSERT_INFO_OCCURRENCE_HDFS, hiveTableName + HDFS_POST,
      COMMA_JOINER.join(selectHdfsTableColumns()), hiveTableName + HBASE_POST);
  }

  /**
   * Generates the drop table statements for the hdfs and hbase backed tables.
   */
  private static String buildDropTableStatements(String hiveTableName) {
    return String.format(DROP_TABLE_FMT, hiveTableName + HDFS_POST) + '\n'
      + String.format(DROP_TABLE_FMT, hiveTableName + HBASE_POST);
  }

  /**
   * Generates a Hive script that deletes the hive occurrence tables, creates them and populate the HDFS table.
   */
  public static String generateHiveScript(String hiveTableName, String hbaseTableName) {
    return HIVE_DEFAULT_OPTS + '\n' + buildDropTableStatements(hiveTableName) + '\n'
      + buildCreateHdfsTable(hiveTableName) + '\n' + buildCreateHBaseTable(hiveTableName, hbaseTableName)
      + '\n' + HIVE_MULTIMEDIA_TABLE + '\n' + buildInsertFromHBaseIntoHive(hiveTableName)
      + '\n' + String.format(INSERT_MULTIMEDIA_FMT, hiveTableName + HDFS_POST);
  }

  /**
   * Entry point.
   * Requires 2 arguments: the Hive table name and the HBase table name.
   * The hive table name, preferably, shouldn't contain 'hdfs' at the end because the script by default add it.
   * Optional argument: output file, if the script is generated into a file.
   */
  public static void main(String[] args) throws IOException {
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
    System.out.println(DownloadTableGenerator.generateHiveScript(hiveTableName, hbaseTableName));
    closer.close();
  }
}
