package org.gbif.occurrence.hive.udf;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.occurrence.processor.interpreting.util.NubLookupInterpreter;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.internal.Lists;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


/**
 * A UDF that uses the GBIF API to lookup from the backbone taxonomy.
 */
@Description(
  name = "nubLookup",
  value = "_FUNC_(kingdom, phylum, class, order, family, genus, scientificName, author)")
public class NubLookupUDF extends GenericUDF {

  private String clean(Object object) {
    if (object != null) {
      return ClassificationUtils.clean(object.toString());
    }
    return null;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert arguments.length == 8;

    String k = clean(arguments[0].get());
    String p = clean(arguments[1].get());
    String c = clean(arguments[2].get());
    String o = clean(arguments[3].get());
    String f = clean(arguments[4].get());
    String g = clean(arguments[5].get());
    String s = clean(arguments[6].get());
    String a = clean(arguments[7].get());

    String name = ObjectUtils.firstNonNull(k, p, c, o, f, g, s, a);
    List<Object> result = Lists.newArrayList(17);

    if (name != null) {
      // LOG.info("Lookup k[{}] p[{}] c[{}] o[{}] f[{}] g[{}] s[{}] a[{}]", k, p, c, o, f, g, s, a);
      ParseResult<NameUsageMatch> response = NubLookupInterpreter.nubLookup(k, p, c, o, f, g, s, a);

      if (response == null || !response.isSuccessful()) {
        // Perhaps we should consider a retry mechanism?
      } else if (response.getPayload() != null) {
        NameUsageMatch lookup = response.getPayload();
        result.add(lookup.getUsageKey());
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
        result.add(lookup.getScientificName());
        result.add(lookup.getConfidence());
      }
    }

    return result;
  }

  @Override
  public String getDisplayString(String[] strings) {
    assert strings.length == 8;
    return "nubLookup(" + strings[0] + ", " + strings[1] + ", " + strings[2] + ", " + strings[3] + ", " + strings[4]
      + ", " + strings[5] + ", " + strings[6] + ", " + strings[7] + ')';
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 8) {
      throw new UDFArgumentException("nubLookup takes eight arguments");
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(Arrays
      .asList("nubId", "nubKingdomId", "nubPhylumId", "nubClassId", "nubOrderId", "nubFamilyId", "nubGenusId",
        "nubSpeciesId", "kingdom", "phylum", "classRank", "orderRank", "family", "genus",
        "species", "scientificName", "confidence"), Arrays
      .<ObjectInspector>asList(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
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
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector));
  }
}
