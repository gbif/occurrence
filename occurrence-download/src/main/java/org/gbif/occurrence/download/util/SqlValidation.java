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

import calcite_gbif_shaded.com.google.common.collect.ImmutableMap;
import calcite_gbif_shaded.org.apache.calcite.rel.type.RelDataType;
import calcite_gbif_shaded.org.apache.calcite.rel.type.RelDataTypeFactory;
import calcite_gbif_shaded.org.apache.calcite.rel.type.RelDataTypeSystem;
import calcite_gbif_shaded.org.apache.calcite.schema.SchemaPlus;
import calcite_gbif_shaded.org.apache.calcite.schema.impl.AbstractTable;
import calcite_gbif_shaded.org.apache.calcite.sql.SqlFunction;
import calcite_gbif_shaded.org.apache.calcite.sql.SqlFunctionCategory;
import calcite_gbif_shaded.org.apache.calcite.sql.SqlKind;
import calcite_gbif_shaded.org.apache.calcite.sql.SqlOperator;
import calcite_gbif_shaded.org.apache.calcite.sql.type.OperandTypes;
import calcite_gbif_shaded.org.apache.calcite.sql.type.ReturnTypes;
import calcite_gbif_shaded.org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import calcite_gbif_shaded.org.apache.calcite.sql.type.SqlTypeFamily;
import calcite_gbif_shaded.org.apache.calcite.sql.type.SqlTypeName;
import calcite_gbif_shaded.org.apache.calcite.tools.Frameworks;
import org.gbif.occurrence.download.hive.HiveDataTypes;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;
import org.gbif.occurrence.query.sql.HiveSqlQuery;
import org.gbif.occurrence.query.sql.HiveSqlValidator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlValidation {

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
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    OccurrenceTable testTable = new OccurrenceTable("occurrence");
    rootSchema.add(testTable.getTableName(), testTable);

    List<SqlOperator> additionalOperators = new ArrayList<>();

    // Built-in Hive function
    additionalOperators.add(new SqlFunction("array_contains",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.ANY),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.ContainsUDF
    additionalOperators.add(new SqlFunction("gbif_within",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.EeaCellCodeUDF
    additionalOperators.add(new SqlFunction("gbif_eeaCellCode",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.GeoDistanceUDF
    additionalOperators.add(new SqlFunction("gbif_geoDistance",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.ToISO8601UDF
    additionalOperators.add(new SqlFunction("gbif_toISO8601",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      OperandTypes.family(SqlTypeFamily.ANY),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // org.gbif.occurrence.hive.udf.ToLocalISO8601UDF
    additionalOperators.add(new SqlFunction("gbif_toLocalISO8601",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      OperandTypes.family(SqlTypeFamily.ANY),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    // brickhouse.udf.collect.JoinArrayUDF
    additionalOperators.add(new SqlFunction("gbif_joinArray",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    hiveSqlValidator = new HiveSqlValidator(rootSchema, additionalOperators);
  }

  public HiveSqlQuery validateAndParse(String sql) {
    return new HiveSqlQuery(hiveSqlValidator, sql);
  }

  /**
   * Table definition for testing
   */
  class OccurrenceTable extends AbstractTable {

    private final String tableName;

    public OccurrenceTable(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();

      OccurrenceHDFSTableDefinition.definition().stream().forEach(
        field -> {
          switch (field.getHiveDataType()) {
            case HiveDataTypes.TYPE_ARRAY_STRING:
              RelDataTypeFactory tdf = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
              RelDataType array = tdf.createArrayType(tdf.createSqlType(SqlTypeName.VARCHAR), -1);
              builder.add(field.getHiveField(), array);
              break;
            case HiveDataTypes.TYPE_VOCABULARY_STRUCT:
              // TODO: "STRUCT<concept: STRING,lineage: ARRAY<STRING>>";
              break;
            case HiveDataTypes.TYPE_ARRAY_PARENT_STRUCT:
              // TODO: "ARRAY<STRUCT<id: STRING,eventType: STRING>>";
              break;

            default:
              builder.add(field.getHiveField(), HIVE_TYPE_MAPPING.get(field.getHiveDataType()));
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
