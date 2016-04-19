package org.gbif.occurrence.solr;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.hive.HiveColumns;
import org.gbif.occurrence.download.hive.HiveDataTypes;
import org.gbif.occurrence.search.writer.FullTextFieldBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/**
 * This provides the definition required to construct the occurrence hdfs table, for use as a Hive table.
 * The table is populated by a query which scans the HBase backed table, but along the way converts some fields to
 * e.g. Hive arrays which requires some UDF voodoo captured here.
 * <p/>
 * Note to developers: It is not easy to find a perfectly clean solution to this work.  Here we try and favour long
 * term code management over all else.  For that reason, Functional programming idioms are not used even though they
 * would reduce lines of code.  However, they come at a cost in that there are several levels of method chaining
 * here, and it becomes difficult to follow as they are not so intuitive on first glance.  Similarly, we don't attempt
 * to push all complexities into the freemarker templates (e.g. complex UDF chaining) as it becomes unmanageable.
 * <p/>
 * Developers please adhere to the above design goals when modifying this class, and consider developing for simple
 * maintenance.
 */
public class OccurrenceSearchFieldsDefinition {

  private static final Set<String> NON_SEARCHABLE_HIVE_TYPES = ImmutableSet.<String>of(HiveDataTypes.TYPE_BIGINT,
                                                                                       HiveDataTypes.TYPE_ARRAY_STRING,
                                                                                       HiveDataTypes.TYPE_INT,
                                                                                       HiveDataTypes.TYPE_BOOLEAN);

  private static final Set<Term> TEMPORAL_FIELDS = ImmutableSet.<Term>of(DwcTerm.year, DwcTerm.month, DwcTerm.day);

  private static final Predicate<Term> NON_SEARCHABLE_TYPES = new Predicate<Term>() {
    @Override
    public boolean apply(@Nullable Term input) {
      return !NON_SEARCHABLE_HIVE_TYPES.contains(HiveDataTypes.typeForTerm(input, false))
             //don't discard temporal INT fields: year, month and day
             || (HiveDataTypes.typeForTerm(input, false) == HiveDataTypes.TYPE_INT && TEMPORAL_FIELDS.contains(input));
    }
  };

  private static final Set<Term> SEARCH_FIELDS = Sets.difference(Sets.filter(DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS, NON_SEARCHABLE_TYPES),
                                                                 FullTextFieldBuilder.NON_FULL_TEXT_TERMS);

  private static final String HIVE_OUT_DIR = "hive-scripts/";

  /**
   * Private constructor.
   */
  private OccurrenceSearchFieldsDefinition(){
    //hidden constructor
  }

  /**
   * Generates the conceptual definition for the occurrence tables when used in hive.
   *
   * @return a list of fields, with the types.
   */
  public static Collection<String> definition() {
    return Collections2.transform(SEARCH_FIELDS, new Function<Term, String>() {
      @Override
      public String apply(@Nullable Term input) {
        return HiveColumns.columnFor(input);
      }
    });
  }

  /**
   * Generates HQL which create a Hive table on the HBase table.
   */
  private static void generateHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "import_hive_to_avro.q"))) {
      Template template = cfg.getTemplate("import_hive_to_avro.ftl");
      Map<String, Object> data = ImmutableMap.<String, Object>of("fields", definition());
      template.process(data, out);
    }
  }

  public static void main(String[] args) {
    try {
      Preconditions.checkState(1 == args.length, "Output path for HQL files is required");
      File outDir = new File(args[0]);
      Preconditions.checkState(outDir.exists() && outDir.isDirectory(), "Output directory must exist");

      // create the sub directories into which we will write
      File createTablesDir = new File(outDir, HIVE_OUT_DIR);
      createTablesDir.mkdirs();


      Configuration cfg = new Configuration();
      cfg.setTemplateLoader(new ClassTemplateLoader(OccurrenceSearchFieldsDefinition.class, "/templates"));

      // generates HQL for the coordinator jobs to create the tables to be queried
      generateHQL(cfg, createTablesDir);

    } catch (Exception e) {
      // Hard exit for safety, and since this is used in build pipelines, any generation error could have
      // catastophic effects - e.g. partially complete scripts being run, and resulting in inconsistent
      // data.
      System.err.println("*** Aborting JVM ***");
      System.err.println("Unexpected error building the templated HQL files.  "
                         + "Exiting JVM as a precaution, after dumping technical details.");
      e.printStackTrace();
      System.exit(-1);
    }
  }


}
