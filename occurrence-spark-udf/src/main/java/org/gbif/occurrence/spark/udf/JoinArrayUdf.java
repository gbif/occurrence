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
package org.gbif.occurrence.spark.udf;

import org.apache.spark.sql.api.java.UDF2;

import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

public class JoinArrayUdf implements UDF2<WrappedArray<String>,String,String> {
  @Override
  public String call(WrappedArray<String> stringWrappedArray, String sep) throws Exception {
    return (stringWrappedArray != null && !stringWrappedArray.isEmpty())?  String.join(sep, JavaConverters.asJavaCollection(stringWrappedArray)) : null;
  }
}
