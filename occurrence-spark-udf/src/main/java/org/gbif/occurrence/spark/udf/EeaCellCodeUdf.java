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

import org.gbif.occurrence.cube.functions.EeaCellCode;
import static org.gbif.occurrence.spark.udf.ConvertionUtils.toDouble;

import org.apache.spark.sql.api.java.UDF4;

/**
 * Randomize a point according to its coordinateUncertainty (or some other distance), and determine the
 * EEA Reference Grid Cell in which the randomized point lies.
 */
public class EeaCellCodeUdf implements UDF4<Integer,Double,Double,Number,String> {

  private final EeaCellCode eeaCellCode = new EeaCellCode();

  @Override
  public String call(Integer gridSize, Double lat, Double lon, Number coordinateUncertaintyInMeters) throws Exception {
      return eeaCellCode.fromCoordinate(gridSize, lat, lon,toDouble(coordinateUncertaintyInMeters));
  }
}
