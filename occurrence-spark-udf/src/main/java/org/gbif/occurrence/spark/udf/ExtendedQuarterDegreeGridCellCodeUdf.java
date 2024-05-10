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

import org.gbif.occurrence.cube.functions.ExtendedQuarterDegreeGridCellCode;

import org.apache.spark.sql.api.java.UDF4;

public class ExtendedQuarterDegreeGridCellCodeUdf implements UDF4<Integer,Double,Double,Double,String> {

  private final ExtendedQuarterDegreeGridCellCode extendedQuarterDegreeGridCellCode = new ExtendedQuarterDegreeGridCellCode();

  @Override
  public String call(Integer level, Double lat, Double lon, Double coordinateUncertaintyInMeters) throws Exception {
     return extendedQuarterDegreeGridCellCode.fromCoordinate(level, lat, lon, coordinateUncertaintyInMeters);
  }
}