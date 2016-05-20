package org.gbif.occurrence.hive.udf;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

/**
 * A simple UDF that remove nulls from a list.
 */
public class ArrayNullsRemoverGenericUDF extends GenericUDF {

  private StandardListObjectInspector retValInspector;
  private PrimitiveObjectInspector primitiveObjectInspector;

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    List list = retValInspector.getList(arguments[0].get());
    List result = Lists.newArrayList();
    if (list != null && !list.isEmpty()) {
      if (primitiveObjectInspector != null && primitiveObjectInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        result = handlerStringArray(list);
      } else {
        result = Lists.newArrayList(list);
        result.removeAll(Collections.singleton(null));
      }
    }
    return result.isEmpty() ? null : result;
  }

  /**
   * Removes empty Strings and null values.
   */
  private List handlerStringArray(List list) {
    List result = Lists.newArrayList();
    for (Object oElement : list) {
      Object stdObject = ObjectInspectorUtils.copyToStandardJavaObject(oElement, primitiveObjectInspector);
      if (stdObject != null && !((String)stdObject).trim().isEmpty()) {
        result.add(stdObject);
      }
    }
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "removeNulls( " + children[0] + ")";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("removeNulls takes an array as argument");
    }
    if (arguments[0].getCategory() != Category.LIST) {
      throw new UDFArgumentException("removeNulls takes an array as argument");
    }
    retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(arguments[0]);
    if (retValInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
      primitiveObjectInspector = (PrimitiveObjectInspector) retValInspector.getListElementObjectInspector();
    }
    return retValInspector;
  }

}
