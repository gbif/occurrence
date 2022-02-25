/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.hive.udf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.io.BooleanWritable;

/**
 * A simple UDF that remove nulls from a list.
 */
public class StringArrayContainsGenericUDF extends GenericUDF {

  private StandardListObjectInspector retValInspector;
  private PrimitiveObjectInspector primitiveObjectInspector;
  private BooleanWritable result;

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    List arrayValues = retValInspector.getList(arguments[0].get());
    String value = arguments[1].get().toString();
    boolean caseSensitive = Boolean.parseBoolean(arguments[2].get().toString());

    if (arrayValues == null || arrayValues.isEmpty()) {
      result.set(false);
      return result;
    }

    String lowerValue = value.toLowerCase();
    for (Object oElement : arrayValues) {
      Object stdObject =
          ObjectInspectorUtils.copyToStandardJavaObject(oElement, primitiveObjectInspector);
      if (stdObject != null && !((String) stdObject).trim().isEmpty()) {
        String arrayVal = (String) stdObject;

        boolean equal =
            caseSensitive ? arrayVal.equals(value) : arrayVal.toLowerCase().equals(lowerValue);

        if (equal) {
          result.set(true);
          return result;
        }
      }
    }

    result.set(false);
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "stringArrayContains( " + children[0] + "," + children[1] + "," + children[2] + ")";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 3) {
      throw new UDFArgumentException("stringArrayContains takes an array, a value and a boolean as argument");
    }
    if (arguments[0].getCategory() != Category.LIST) {
      throw new UDFArgumentException("stringArrayContains takes an array as first argument");
    }
    if (!arguments[1].getTypeName().equals("string")) {
      throw new UDFArgumentTypeException(0, "stringArrayContains takes a string as second argument");
    }
    if (!arguments[2].getTypeName().equals("boolean")) {
      throw new UDFArgumentTypeException(0, "stringArrayContains takes a boolean as third argument");
    }

    retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(arguments[0]);
    if (retValInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
      primitiveObjectInspector = (PrimitiveObjectInspector) retValInspector.getListElementObjectInspector();
    }

    result = new BooleanWritable(false);

    return retValInspector;
  }

}
