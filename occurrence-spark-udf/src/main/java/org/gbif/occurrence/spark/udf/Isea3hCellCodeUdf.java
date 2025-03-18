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

import org.gbif.occurrence.cube.functions.Isea3hCellCode;

import org.apache.spark.sql.api.java.UDF4;
import static org.gbif.occurrence.spark.udf.ConvertionUtils.toDouble;

/**
 * Randomize a point according to its coordinateUncertainty (or some other distance), and determine the
 * ISEA3H Cell in which the randomized point lies at the given resolution.
 */
public class Isea3hCellCodeUdf implements UDF4<Integer,Double,Double,Number,String> {

  private final Isea3hCellCode isea3hCellCode = new Isea3hCellCode();

  @Override
  public String call(Integer level, Double lat, Double lon, Number coordinateUncertaintyInMeters) throws Exception {
    return isea3hCellCode.fromCoordinate(level, lat, lon, toDouble(coordinateUncertaintyInMeters));
  }
}
