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
package org.gbif.occurrence.download.util;

import org.gbif.api.exception.QueryBuildingException;
import org.gbif.occurrence.download.hive.HiveDataTypes;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;
import org.gbif.occurrence.query.sql.HiveSqlQuery;
import org.gbif.occurrence.query.sql.HiveSqlValidator;

import java.util.*;

import calcite_gbif_shaded.com.google.common.collect.ImmutableMap;
import calcite_gbif_shaded.org.apache.calcite.rel.type.RelDataType;
import calcite_gbif_shaded.org.apache.calcite.rel.type.RelDataTypeFactory;
import calcite_gbif_shaded.org.apache.calcite.rel.type.RelDataTypeSystem;
import calcite_gbif_shaded.org.apache.calcite.rel.type.StructKind;
import calcite_gbif_shaded.org.apache.calcite.schema.SchemaPlus;
import calcite_gbif_shaded.org.apache.calcite.schema.Table;
import calcite_gbif_shaded.org.apache.calcite.schema.impl.AbstractSchema;
import calcite_gbif_shaded.org.apache.calcite.schema.impl.AbstractTable;
import calcite_gbif_shaded.org.apache.calcite.sql.SqlFunction;
import calcite_gbif_shaded.org.apache.calcite.sql.SqlFunctionCategory;
import calcite_gbif_shaded.org.apache.calcite.sql.SqlKind;
import calcite_gbif_shaded.org.apache.calcite.sql.SqlOperator;
import calcite_gbif_shaded.org.apache.calcite.sql.type.*;
import calcite_gbif_shaded.org.apache.calcite.tools.Frameworks;

import static calcite_gbif_shaded.org.apache.calcite.sql.type.OperandTypes.family;

public class SqlValidation {

  //Spark/Hive Catalog
  private static final String CATALOG = "iceberg";

  private final String database;

  private static final Map<String, SqlTypeName> HIVE_TYPE_MAPPING = ImmutableMap.<String, SqlTypeName>builder()
    .put(HiveDataTypes.TYPE_STRING, SqlTypeName.VARCHAR)
    .put(HiveDataTypes.TYPE_BOOLEAN, SqlTypeName.BOOLEAN)
    .put(HiveDataTypes.TYPE_INT, SqlTypeName.INTEGER)
    .put(HiveDataTypes.TYPE_DOUBLE, SqlTypeName.DOUBLE)
    .put(HiveDataTypes.TYPE_BIGINT, SqlTypeName.BIGINT)
    .put(HiveDataTypes.TYPE_TIMESTAMP, SqlTypeName.TIMESTAMP)
    .build();

  private final HiveSqlValidator hiveSqlValidator;

  public SqlValidation() {
    this(null);
  }

  public SqlValidation(String database) {
    this.database = database;
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    OccurrenceTable occurrenceTable = new OccurrenceTable("occurrence");
    rootSchema.add(occurrenceTable.getTableName(), occurrenceTable);
    if (database != null) {
      rootSchema.add(CATALOG + "." + database, new AbstractSchema() {
        @Override
        protected Map<String, Table> getTableMap() {
          return Collections.singletonMap("occurrence", occurrenceTable);
        }
      });
    }

    List<SqlOperator> additionalOperators = new ArrayList<>();

    // org.gbif.occurrence.hive.udf.ContainsUDF
    additionalOperators.add(new SqlFunction("gbif_within",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      family(SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.DmsCellCodeUDF
    additionalOperators.add(new SqlFunction("gbif_DMSGCode",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.EeaCellCodeUDF
    additionalOperators.add(new SqlFunction("gbif_EEARGCode",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.ExtendedQuarterDegreeGridCellCodeUDF
    additionalOperators.add(new SqlFunction("gbif_EQDGCCode",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.GeoDistanceUDF
    additionalOperators.add(new SqlFunction("gbif_geoDistance",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.Isea3hCellCodeUDF
    additionalOperators.add(new SqlFunction("gbif_ISEA3HCode",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.MilitaryGridReferenceSystemCellCodeUDF
    additionalOperators.add(new SqlFunction("gbif_MGRSCode",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.TemporalUncertaintyUDF
    additionalOperators.add(new SqlFunction("gbif_TemporalUncertainty",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.INTEGER,
      null,
      OperandTypes.STRING_OPTIONAL_STRING,
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.MillisecondsToISO8601UDF
    additionalOperators.add(new SqlFunction("gbif_millisecondsToISO8601",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      family(SqlTypeFamily.ANY),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.SecondsToISO8601UDF
    additionalOperators.add(new SqlFunction("gbif_secondsToISO8601",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      family(SqlTypeFamily.ANY),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.SecondsToLocalISO8601UDF
    additionalOperators.add(new SqlFunction("gbif_secondsToLocalISO8601",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      family(SqlTypeFamily.ANY),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.spark.udf.StringArrayContainsGenericUdf
    additionalOperators.add(new SqlFunction("gbif_stringArrayContains",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      family(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER, SqlTypeFamily.BOOLEAN),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.spark.udf.StringArrayLikeGenericUdf
    additionalOperators.add(new SqlFunction("gbif_stringArrayLike",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      family(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER, SqlTypeFamily.BOOLEAN),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    hiveSqlValidator = new HiveSqlValidator(rootSchema, additionalOperators);
  }

  public HiveSqlQuery validateAndParse(String sql, boolean addCatalog) throws QueryBuildingException {
    if (addCatalog) {
      String databaseFq = database == null ? CATALOG : CATALOG + "." + database;
      return new HiveSqlQuery(hiveSqlValidator, sql, databaseFq);
    } else {
      return new HiveSqlQuery(hiveSqlValidator, sql);
    }
  }

  /**
   * Occurrence table definition for validation
   */
  class OccurrenceTable extends AbstractTable {

    private final String tableName;

    public OccurrenceTable(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      final RelDataTypeFactory tdf = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataTypeFactory.Builder builder = typeFactory.builder();

      RelDataType varChar = tdf.createSqlType(SqlTypeName.VARCHAR);
      RelDataType doubleType = tdf.createSqlType(SqlTypeName.DOUBLE);

      // String array definition
      RelDataType varCharArray = tdf.createArrayType(varChar, -1);

      // Vocabulary definition: "STRUCT<concept: STRING,lineage: ARRAY<STRING>>"
      RelDataType vocabulary = tdf.createStructType(StructKind.PEEK_FIELDS_NO_EXPAND,
        Arrays.asList(varChar, varCharArray),
        Arrays.asList("concept", "lineage"));

      // Vocabulary array definition: "STRUCT<concepts: ARRAY<STRING>,lineage: ARRAY<STRING>>"
      RelDataType vocabularyArray = tdf.createStructType(StructKind.PEEK_FIELDS_NO_EXPAND,
        Arrays.asList(varCharArray, varCharArray),
        Arrays.asList("concepts", "lineage"));

      // Array of key-value pairs: ARRAY<STRUCT<id: STRING,eventType: STRING>>
      RelDataType keyValuePair = tdf.createStructType(Arrays.asList(
        new AbstractMap.SimpleEntry<>("id", varChar),
        new AbstractMap.SimpleEntry<>("eventType", varChar)));
      RelDataType parentEventGbifId = tdf.createArrayType(keyValuePair, -1);

      // Geological range structure: STRUCT<gt: DOUBLE,lte: DOUBLE>
      RelDataType geologicalRange = tdf.createStructType(StructKind.PEEK_FIELDS_NO_EXPAND,
        Arrays.asList(doubleType, doubleType),
        Arrays.asList("gt", "lte"));

      OccurrenceHDFSTableDefinition.definition().stream().forEach(
        field -> {
          switch (field.getHiveDataType()) {
            case HiveDataTypes.TYPE_ARRAY_STRING:
              builder.add(field.getColumnName(), varCharArray);
              break;

            case HiveDataTypes.TYPE_VOCABULARY_STRUCT:
              // lifeStage, eventType, earlistEonOrLowestEonotherm, etc.
              builder.add(field.getColumnName(), vocabulary);
              break;

            case HiveDataTypes.TYPE_VOCABULARY_ARRAY_STRUCT:
              // typeStatus.
              builder.add(field.getColumnName(), vocabularyArray);
              break;

            case HiveDataTypes.TYPE_ARRAY_PARENT_STRUCT:
              // Currently only parentEventGbifId, which doesn't seem to be set.
              builder.add(field.getColumnName(), parentEventGbifId);
              break;

            case HiveDataTypes.GEOLOGICAL_RANGE_STRUCT:
              // geologicalTime
              builder.add(field.getColumnName(), parentEventGbifId);
              break;

            default:
              builder.add(field.getColumnName(), HIVE_TYPE_MAPPING.get(field.getHiveDataType()));
          }
        }
      );

      return builder.build();
    }

    public String getTableName() {
      return tableName;
    }
  }
}
