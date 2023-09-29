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
package org.gbif.occurrence.trino.udf;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import lombok.SneakyThrows;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.common.parsers.BasisOfRecordParser;
import org.gbif.common.parsers.core.ParseResult;

public class BasisOfRecordParseUDF {

  private static final BasisOfRecordParser BOR_PARSER = BasisOfRecordParser.getInstance();

  @SneakyThrows
  @ScalarFunction(value = "parseBoR", deterministic = true)
  @Description("Parses a basis of record string")
  @SqlType(StandardTypes.VARCHAR)
  public Slice parseBoR(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice bor) {
    if (bor != null) {
      ParseResult<BasisOfRecord> parsed = BOR_PARSER.parse(bor.toStringUtf8());
      if (parsed.isSuccessful()) {
        return Slices.utf8Slice(parsed.getPayload().toString());
      }
    }

    return Slices.utf8Slice(BasisOfRecord.OCCURRENCE.toString());
  }
}
