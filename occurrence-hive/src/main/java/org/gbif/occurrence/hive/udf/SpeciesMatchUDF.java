package org.gbif.occurrence.hive.udf;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.occurrence.processor.guice.ApiClientConfiguration;
import org.gbif.occurrence.processor.interpreting.TaxonomyInterpreter;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A UDF to run a backbone species match against the GBIF API.
 * The UDF is lazily initialized with the base URL of the API to be used.
 * Within the same JVM the UDF will only ever use the first URL used and ignores subsequently changed URLs.
 */
@Description(
  name = "match",
  value = "_FUNC_(apiUrl, kingdom, phylum, class, order, family, genus, scientificName, specificEpithet, infraspecificEpithet)")
public class SpeciesMatchUDF extends GenericUDF {
  private static final Logger LOG = LoggerFactory.getLogger(SpeciesMatchUDF.class);

  private static final int argLength = 10;
  private static final Joiner joinComma = Joiner.on(",").useForNull("-");

  private TaxonomyInterpreter taxonomyInterpreter;
  private Object lock = new Object();

  public TaxonomyInterpreter getInterpreter(URI apiWs) {
    TaxonomyInterpreter ti = taxonomyInterpreter;
    if(ti == null) {
      synchronized(lock) {    // while we were waiting for the lock, another thread may have instantiated the object
        ti = taxonomyInterpreter;
        if (ti == null) {
          LOG.info("Create new species match client using API at {}", apiWs);
          ApiClientConfiguration cfg = new ApiClientConfiguration();
          cfg.url = apiWs;
          ti = new TaxonomyInterpreter(cfg);
          taxonomyInterpreter = ti;
        }
      }
    }
    return ti;
  }

  private String clean(Object object) {
    if (object != null) {
      return ClassificationUtils.clean(object.toString());
    }
    return null;
  }

  @Override
  public void configure(MapredContext context) {
    super.configure(context);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert arguments.length == argLength;

    URI api = URI.create(arguments[0].get().toString().toUpperCase());

    String k = clean(arguments[1].get());
    String p = clean(arguments[2].get());
    String c = clean(arguments[3].get());
    String o = clean(arguments[4].get());
    String f = clean(arguments[5].get());
    String g = clean(arguments[6].get());
    String s = clean(arguments[7].get());
    String sp = clean(arguments[8].get());
    String ssp = clean(arguments[9].get());

    List<Object> result = Lists.newArrayList(20);
    ParseResult<NameUsageMatch> response = getInterpreter(api).match(k, p, c, o, f, g, s, sp, ssp);

    if (response != null && response.getPayload() != null) {
      NameUsageMatch lookup = response.getPayload();
      result.add(lookup.getUsageKey());
      result.add(lookup.getScientificName());
      result.add(lookup.getRank());
      result.add(lookup.getStatus());
      result.add(lookup.getMatchType());
      result.add(lookup.getConfidence());

      result.add(lookup.getKingdomKey());
      result.add(lookup.getPhylumKey());
      result.add(lookup.getClassKey());
      result.add(lookup.getOrderKey());
      result.add(lookup.getFamilyKey());
      result.add(lookup.getGenusKey());
      result.add(lookup.getSpeciesKey());

      result.add(lookup.getKingdom());
      result.add(lookup.getPhylum());
      result.add(lookup.getClazz());
      result.add(lookup.getOrder());
      result.add(lookup.getFamily());
      result.add(lookup.getGenus());
      result.add(lookup.getSpecies());
    }

    return result;
  }

  @Override
  public String getDisplayString(String[] strings) {
    assert strings.length == argLength;
    return "match(" + joinComma.join(strings) + ')';
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != argLength) {
      throw new UDFArgumentException("nubLookup takes "+ argLength +" arguments");
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(Arrays
      .asList("taxonKey", "scientificName", "rank", "status", "matchType", "confidence",
        "kingdomKey", "phylumKey", "classKey", "orderKey", "familyKey", "genusKey", "speciesKey",
        "kingdom", "phylum", "class_", "order_", "family", "genus", "species"),
      Arrays.<ObjectInspector>asList(
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,

        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,

        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector
      )
    );
  }
}
