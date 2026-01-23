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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import lombok.experimental.UtilityClass;
import org.gbif.api.model.occurrence.SqlDownloadFunction;

@UtilityClass
public class UDFS {

  public static void registerUdfs(SparkSession sparkSession) {
    sparkSession.udf().register("cleanDelimiters", new CleanDelimiterCharsUdf(), DataTypes.StringType);
    sparkSession.udf().register("cleanDelimitersArray", new CleanDelimiterArraysUdf(), DataTypes.createArrayType(DataTypes.StringType));
    sparkSession.udf().register("secondsToISO8601", new SecondsToISO8601Udf(), DataTypes.StringType);
    sparkSession.udf().register("secondsToLocalISO8601", new SecondsToLocalISO8601Udf(), DataTypes.StringType);
    sparkSession.udf().register("millisecondsToISO8601", new MillisecondsToISO8601Udf(), DataTypes.StringType);
    sparkSession.udf().register("stringArrayContains", new StringArrayContainsGenericUdf(), DataTypes.BooleanType);
    sparkSession.udf().register("stringArrayLike", new StringArrayLikeGenericUdf(), DataTypes.BooleanType);
    sparkSession.udf().register("contains", new ContainsUdf(), DataTypes.BooleanType);
    sparkSession.udf().register("geoDistance", new GeoDistanceUdf(), DataTypes.BooleanType);

    // SQL Downloads — public-visible names.
    sparkSession.udf().register(SqlDownloadFunction.DEGREE_MINUTE_SECOND_GRID_CELL_CODE.getSqlIdentifier(), new DegreeMinuteSecondGridCellCodeUdf(), DataTypes.StringType);
    sparkSession.udf().register(SqlDownloadFunction.EEA_CELL_CODE.getSqlIdentifier(), new EeaCellCodeUdf(), DataTypes.StringType);
    sparkSession.udf().register(SqlDownloadFunction.EUROSTAT_CELL_CODE.getSqlIdentifier(), new EuroStatCellCodeUdf(), DataTypes.StringType);
    sparkSession.udf().register(SqlDownloadFunction.ISEA3H_CELL_CODE.getSqlIdentifier(), new Isea3hCellCodeUdf(), DataTypes.StringType);
    sparkSession.udf().register(SqlDownloadFunction.MILITARY_GRID_REFERENCE_SYSTEM_CELL_CODE.getSqlIdentifier(), new MilitaryGridReferenceSystemCellCodeUdf(), DataTypes.StringType);
    sparkSession.udf().register(SqlDownloadFunction.EXTENDED_QUARTER_DEGREE_GRID_CELL_CODE.getSqlIdentifier(), new ExtendedQuarterDegreeGridCellCodeUdf(), DataTypes.StringType);
    sparkSession.udf().register(SqlDownloadFunction.TEMPORAL_UNCERTAINTY.getSqlIdentifier(), new TemporalUncertaintyUdf(), DataTypes.LongType);
    sparkSession.udf().register(SqlDownloadFunction.GEO_DISTANCE.getSqlIdentifier(), new GeoDistanceUdf(), DataTypes.BooleanType);
    sparkSession.udf().register(SqlDownloadFunction.MILLISECONDS_TO_ISO8601.getSqlIdentifier(), new MillisecondsToISO8601Udf(), DataTypes.StringType);
    sparkSession.udf().register(SqlDownloadFunction.SECONDS_TO_ISO8601.getSqlIdentifier(), new SecondsToISO8601Udf(), DataTypes.StringType);
    sparkSession.udf().register(SqlDownloadFunction.SECONDS_TO_LOCAL_ISO8601.getSqlIdentifier(), new SecondsToLocalISO8601Udf(), DataTypes.StringType);
    sparkSession.udf().register(SqlDownloadFunction.CONTAINS.getSqlIdentifier(), new ContainsUdf(), DataTypes.BooleanType);
    sparkSession.udf().register(SqlDownloadFunction.STRING_ARRAY_CONTAINS_GENERIC.getSqlIdentifier(), new StringArrayContainsGenericUdf(), DataTypes.BooleanType);
    sparkSession.udf().register(SqlDownloadFunction.STRING_ARRAY_LIKE_GENERIC.getSqlIdentifier(), new StringArrayLikeGenericUdf(), DataTypes.BooleanType);
  }
}
