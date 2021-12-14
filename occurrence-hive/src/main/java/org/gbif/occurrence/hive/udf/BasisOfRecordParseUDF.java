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

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.common.parsers.BasisOfRecordParser;
import org.gbif.common.parsers.core.ParseResult;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Parses raw basis of record into a textual version.
 */
@Description(
  name = "parseBoR",
  value = "_FUNC_(bor)")
public class BasisOfRecordParseUDF extends UDF {

  private static final BasisOfRecordParser BOR_PARSER = BasisOfRecordParser.getInstance();
  private static final Text BOR = new Text();

  public Text evaluate(Text basisOfRecord) {
    BOR.set(BasisOfRecord.OCCURRENCE.toString());
    if (basisOfRecord != null) {
      ParseResult<BasisOfRecord> parsed = BOR_PARSER.parse(basisOfRecord.toString());
      if (parsed.isSuccessful()) {
        BOR.set(parsed.getPayload().toString());
      }
    }

    return BOR;
  }
}
