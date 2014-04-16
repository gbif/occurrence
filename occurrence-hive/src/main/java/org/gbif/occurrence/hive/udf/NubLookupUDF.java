package org.gbif.occurrence.hive.udf;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.occurrence.processor.interpreting.util.NubLookupInterpreter;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.internal.Lists;
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

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert arguments.length == 8;

    String k = arguments[0].get() == null ? null : arguments[0].get().toString();
    String p = arguments[1].get() == null ? null : arguments[1].get().toString();
    String c = arguments[2].get() == null ? null : arguments[2].get().toString();
    String o = arguments[3].get() == null ? null : arguments[3].get().toString();
    String f = arguments[4].get() == null ? null : arguments[4].get().toString();
    String g = arguments[5].get() == null ? null : arguments[5].get().toString();
    String s = arguments[6].get() == null ? null : arguments[6].get().toString();
    String a = arguments[7].get() == null ? null : arguments[7].get().toString();

    ParseResult<NameUsageMatch> response = NubLookupInterpreter.nubLookup(
      ClassificationUtils.clean(k),
      ClassificationUtils.clean(p),
      ClassificationUtils.clean(c),
      ClassificationUtils.clean(o),
      ClassificationUtils.clean(f),
      ClassificationUtils.clean(g),
      ClassificationUtils.clean(s),
      ClassificationUtils.clean(a));

    List<Object> result = Lists.newArrayList(17);
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
