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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BooleanWritable;

/** A UDF that customizes the array contains to take into account case sensitivity. */
public class StringArrayContainsGenericUDF extends GenericUDF {

  private transient ListObjectInspector listObjectInspector;
  private BooleanWritable result;

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object array = arguments[0].get();
    String value = arguments[1].get().toString();
    boolean caseSensitive = Boolean.parseBoolean(arguments[2].get().toString());

    if (array == null) {
      return result;
    }

    int arrayLength = listObjectInspector.getListLength(array);
    if (arrayLength <= 0) {
      return result;
    }

    for (int i = 0; i < arrayLength; ++i) {
      Object listElement = listObjectInspector.getListElement(array, i);

      if (listElement != null && !listElement.toString().trim().isEmpty()) {
        String stringValue = listElement.toString();
        boolean equal =
            caseSensitive ? stringValue.equals(value) : stringValue.equalsIgnoreCase(value);

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
      throw new UDFArgumentException(
          "stringArrayContains takes an array, a value and a boolean as argument");
    }
    if (arguments[0].getCategory() != Category.LIST) {
      throw new UDFArgumentException("stringArrayContains takes an array as first argument");
    }
    if (!(arguments[1] instanceof StringObjectInspector)) {
      throw new UDFArgumentException("stringArrayContains takes a string as second argument");
    }
    if (!(arguments[2] instanceof BooleanObjectInspector)) {
      throw new UDFArgumentException("stringArrayContains takes a boolean as third argument");
    }

    listObjectInspector = (ListObjectInspector) arguments[0];

    result = new BooleanWritable(false);

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }
}
