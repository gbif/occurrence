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

import org.gbif.api.model.Constants;
import org.gbif.api.model.occurrence.SqlDownloadFunction;

import org.gbif.occurrence.query.sql.HiveSqlQuery;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SqlValidationTest {

  @Test
  public void testAllFunctionsMapped() throws Exception {
    assertEquals(SqlDownloadFunction.values().length, SqlValidation.additionalSqlOperators().size());
  }

  @Test
  public void testArrayAggMapped() throws Exception {
    assertEquals(SqlDownloadFunction.values().length, SqlValidation.additionalSqlOperators().size());
    SqlValidation v = new SqlValidation("test", Map.of(Constants.NUB_DATASET_KEY.toString(), "gbif_classification"));
    HiveSqlQuery q = v.validateAndParse("SELECT species, concat_ws(',', ARRAY_AGG(DISTINCT(datasetkey))) AS datasetKeys FROM occurrence where genus = 'Acacia' GROUP BY species", false);
    assertNotNull(q);
  }

  @Test
  public void testNestedStruct() throws Exception {
    assertEquals(SqlDownloadFunction.values().length, SqlValidation.additionalSqlOperators().size());
    SqlValidation v = new SqlValidation("test", Map.of(Constants.NUB_DATASET_KEY.toString(), "gbif_classification"));
    HiveSqlQuery q = v.validateAndParse("SELECT occurrence.gbif_classification.species,  occurrence.gbif_classification.taxonomicissue from occurrence " +
      "WHERE NOT GBIF_STRINGARRAYCONTAINS(occurrence.gbif_classification.taxonomicissue, 'TAXON_MATCH_FUZZY', TRUE)", false);
    assertNotNull(q);
  }

  @Test
  public void testSQL() throws Exception {
    assertEquals(SqlDownloadFunction.values().length, SqlValidation.additionalSqlOperators().size());
    SqlValidation v = new SqlValidation("test", Map.of(Constants.NUB_DATASET_KEY.toString(), "gbif_classification"));
    HiveSqlQuery q = v.validateAndParse("""
      SELECT
        decimallatitude,
        decimallongitude,
        CASE WHEN "year" < 1990 THEN 'pre_1990' ELSE 'post_2000' END AS time_period,
        COUNT(*) AS record_count
      FROM occurrence
      WHERE countrycode = 'CN'
        AND occurrence.gbif_classification.phylumkey = '7707728'
        AND occurrencestatus = 'PRESENT'
        AND decimallatitude IS NOT NULL
        AND decimallongitude IS NOT NULL
        AND hasgeospatialissues = FALSE
        AND "year" IS NOT NULL
        AND ("year" < 1990 OR "year" >= 2000)
      GROUP BY
        decimallatitude,
        decimallongitude,
        CASE WHEN "year" < 1990 THEN 'pre_1990' ELSE 'post_2000' END
      """, false);
    assertNotNull(q);
  }

}
