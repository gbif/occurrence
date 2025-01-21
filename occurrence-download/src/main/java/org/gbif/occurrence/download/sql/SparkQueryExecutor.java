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
package org.gbif.occurrence.download.sql;

import org.apache.spark.sql.SparkSession;

public class SparkQueryExecutor implements QueryExecutor {

  private SparkSessionSupplier sparkSessionSupplier;
  private SparkSession sparkSession = null;

  public SparkQueryExecutor(SparkSessionSupplier sparkSessionSupplier) {
    this.sparkSessionSupplier = sparkSessionSupplier;
  }

  @Override
  public void close() {
    if (sparkSession != null) {
      sparkSession.stop();
      sparkSession.close();
    }
  }

  @Override
  public void accept(String description, String sql) {
    if (sparkSession == null) {
      sparkSession = sparkSessionSupplier.create();
    }
    sparkSession.sparkContext().setJobDescription(description);
    sparkSession.sql(sql);
  }
}
