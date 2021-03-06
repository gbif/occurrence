package org.gbif.occurrence.download.hive;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import org.apache.avro.Schema;

/**
 * This provides the definition required to construct the occurrence hdfs table, for use as a Hive table.
 * The table is populated by a query which scans the Avro files, but along the way converts some fields to
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
public class OccurrenceHDFSTableDefinition {

  private static final String EXT_PACKAGE = "org.gbif.pipelines.io.avro.extension";

  /**
   * Utility class used in the Freemarker template that generates  Hive tables for extensions.
   */
  public static class ExtensionTable {

    private ClassPath.ClassInfo extension;

    private final String leafNamespace;

    public ExtensionTable(ClassPath.ClassInfo extension) {
      this.extension = extension;
      leafNamespace = extension.getPackageName().replace(EXT_PACKAGE + '.',"").replace('.', '_');
    }

    public String getExtension() {
      return extension.getSimpleName();
    }

    private String getLeafNamespace() {
      return leafNamespace;
    }

    public String getHiveTableName() {
      return leafNamespace + '_' + extension.getSimpleName().toLowerCase().replace("table","");
    }

    public String getDirectoryTableName() {
      return extension.getSimpleName().toLowerCase();
    }

    public String getAvroSchemaFileName() {
      return leafNamespace +  '_' + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, extension.getSimpleName()) + ".avsc";
    }

    public String getAvroSchema() {
      try {
        Schema schema = (Schema)extension.load()
          .getDeclaredMethod("getClassSchema")
          .invoke(null, null);
        return schema.toString(true);
      } catch (NoSuchMethodException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
        throw new RuntimeException(ex);
      }
    }

  }

  /**
   * Private constructor.
   */
  private OccurrenceHDFSTableDefinition() {
    //hidden constructor
  }

  /**
   * Assemble the mapping for verbatim fields.
   *
   * @return the list of fields that are used in the verbatim context
   */
  private static List<InitializableField> verbatimFields() {
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (Term t : DownloadTerms.DOWNLOAD_VERBATIM_TERMS) {
      builder.add(verbatimField(t));
    }
    return builder.build();
  }

  private static String cleanDelimitersInitializer(String column) {
    return "cleanDelimiters(" + column + ") AS " + column;
  }

  /**
   * Assemble the mapping for interpreted fields, taking note that in reality, many are mounted onto the verbatim
   * columns.
   *
   * @return the list of fields that are used in the interpreted context
   */
  private static List<InitializableField> interpretedFields() {

    // the following terms are manipulated when transposing from Avro to hive by using UDFs and custom HQL
    Map<Term, String> initializers = ImmutableMap.<Term, String>builder()
                                      .put(GbifTerm.datasetKey, HiveColumns.columnFor(GbifTerm.datasetKey))
                                      .put(GbifTerm.protocol, HiveColumns.columnFor(GbifTerm.protocol))
                                      .put(GbifTerm.publishingCountry, HiveColumns.columnFor(GbifTerm.publishingCountry))
                                      .put(IucnTerm.iucnRedListCategory, HiveColumns.columnFor(IucnTerm.iucnRedListCategory))
                                      .build();

    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (Term t : DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS) {
      // if there is custom handling registered for the term, use it
      if (initializers.containsKey(t)) {
        builder.add(interpretedField(t, initializers.get(t)));
      } else {
        builder.add(interpretedField(t)); // will just use the term name as usual
      }

    }
    return builder.build();
  }

  /**
   * The internal fields stored in Avro which we wish to expose through Hive.  The fragment and fragment hash
   * are removed and not present.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<InitializableField> internalFields() {
    Map<Term, String> initializers = new ImmutableMap.Builder<Term,String>()
                                                      .put(GbifInternalTerm.publishingOrgKey, HiveColumns.columnFor(GbifInternalTerm.publishingOrgKey))
                                                      .put(GbifInternalTerm.installationKey, HiveColumns.columnFor(GbifInternalTerm.installationKey))
                                                      .put(GbifInternalTerm.institutionKey, HiveColumns.columnFor(GbifInternalTerm.institutionKey))
                                                      .put(GbifInternalTerm.collectionKey, HiveColumns.columnFor(GbifInternalTerm.collectionKey))
                                                      .put(GbifInternalTerm.projectId, HiveColumns.columnFor(GbifInternalTerm.projectId))
                                                      .put(GbifInternalTerm.programmeAcronym, HiveColumns.columnFor(GbifInternalTerm.programmeAcronym))
                                                      .put(GbifInternalTerm.hostingOrganizationKey, HiveColumns.columnFor(GbifInternalTerm.hostingOrganizationKey))
                                                      .put(GbifInternalTerm.isInCluster, HiveColumns.columnFor(GbifInternalTerm.isInCluster))
                                                      .put(GbifInternalTerm.dwcaExtension, HiveColumns.columnFor(GbifInternalTerm.dwcaExtension))
                                                      .put(GbifInternalTerm.lifeStageLineage, HiveColumns.columnFor(GbifInternalTerm.lifeStageLineage))
                                            .build();
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (GbifInternalTerm t : GbifInternalTerm.values()) {
      if (!DownloadTerms.EXCLUSIONS.contains(t)) {
        if (initializers.containsKey(t)) {
          builder.add(interpretedField(t, initializers.get(t)));
        } else {
          builder.add(interpretedField(t));
        }
      }
    }
    return builder.build();
  }

  /**
   * The fields stored in Avro which represent an extension.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<InitializableField> extensions() {
    // only MULTIMEDIA is supported, but coded for future use
    Set<Extension> extensions = ImmutableSet.of(Extension.MULTIMEDIA);
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (Extension e : extensions) {
      builder.add(new InitializableField(GbifTerm.Multimedia,
                                         HiveColumns.columnFor(e),
                                         HiveDataTypes.TYPE_STRING
                                         // always, as it has a custom serialization
                  ));
    }
    return builder.build();
  }

  /**
   * Generates the conceptual definition for the occurrence tables when used in hive.
   *
   * @return a list of fields, with the types.
   */
  public static List<InitializableField> definition() {
    return ImmutableList.<InitializableField>builder()
      .add(keyField())
      .addAll(verbatimFields())
      .addAll(internalFields())
      .addAll(interpretedFields())
      .addAll(extensions())
      .build();
  }

  public static List<ExtensionTable> tableExtensions() {
    try {
      ClassPath classPath = ClassPath.from(Thread.currentThread().getContextClassLoader());
      return classPath.getTopLevelClassesRecursive(EXT_PACKAGE).stream()
        .filter(c -> c.getSimpleName().endsWith("Table"))
        .map(ExtensionTable::new)
        .collect(Collectors.toList());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Constructs the field for the primary key, which is a special case in that it needs a special mapping.
   */
  private static InitializableField keyField() {
    return new InitializableField(GbifTerm.gbifID,
                                  HiveColumns.columnFor(GbifTerm.gbifID),
                                  HiveDataTypes.typeForTerm(GbifTerm.gbifID, true)
                                  // verbatim context
    );
  }

  /**
   * Constructs a Field for the given term, when used in the verbatim context.
   */
  private static InitializableField verbatimField(Term term) {
    String column = HiveColumns.VERBATIM_COL_PREFIX + term.simpleName().toLowerCase();
    return new InitializableField(term, column,
                                  // no escape needed, due to prefix
                                  HiveDataTypes.typeForTerm(term, true), // verbatim context
                                  cleanDelimitersInitializer(column) //remove delimiters '\n', '\t', etc.
    );
  }

  /**
   * Constructs a Field for the given term, when used in the interpreted context context constructed with no custom
   * initializer.
   */
  private static InitializableField interpretedField(Term term) {
    if (HiveDataTypes.TYPE_STRING.equals(HiveDataTypes.typeForTerm(term, false))) {
      return interpretedField(term, cleanDelimitersInitializer(HiveColumns.columnFor(term))); // no initializer
    }
    return interpretedField(term, null); // no initializer
  }

  /**
   * Constructs a Field for the given term, when used in the interpreted context context, and setting it up with the
   * given initializer.
   */
  private static InitializableField interpretedField(Term term, String initializer) {
    return new InitializableField(term,
                                  HiveColumns.columnFor(term),
                                  // note that Columns takes care of whether this is mounted
                                  // on a verbatim or an interpreted column for us
                                  HiveDataTypes.typeForTerm(term, false),
                                  // not verbatim context
                                  initializer);
  }
}
