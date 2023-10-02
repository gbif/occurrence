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
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SingleRowBlockWriter;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.RankParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.occurrence.trino.processor.conf.ApiClientConfiguration;
import org.gbif.occurrence.trino.processor.interpreters.TaxonomyInterpreter;

import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * A UDF to run a backbone species match against the GBIF API. The UDF is lazily initialized with
 * the base URL of the API to be used. Within the same JVM the UDF will only ever use the first URL
 * used and ignores subsequently changed URLs.
 */
@Slf4j
public class SpeciesMatchUDF {
  private static final RankParser RANK_PARSER = RankParser.getInstance();

  private TaxonomyInterpreter taxonomyInterpreter;
  private Object lock = new Object();

  public TaxonomyInterpreter getInterpreter(String apiWs) {
    TaxonomyInterpreter ti = taxonomyInterpreter;
    if (ti == null) {
      synchronized (
          lock) { // while we were waiting for the lock, another thread may have instantiated the
        // object
        ti = taxonomyInterpreter;
        if (ti == null) {
          log.info("Create new species match client using API at {}", apiWs);
          ApiClientConfiguration cfg = new ApiClientConfiguration();
          cfg.url = apiWs;
          ti = new TaxonomyInterpreter(cfg);
          taxonomyInterpreter = ti;
        }
      }
    }
    return ti;
  }

  private String clean(Slice arg) {
    if (arg != null) {
      return ClassificationUtils.clean(arg.toStringUtf8());
    }
    return null;
  }

  @ScalarFunction(value = "nubLookup", deterministic = true)
  @Description(
      "A UDF to run a backbone species match against the GBIF API. The UDF is lazily initialized with"
          + " the base URL of the API to be used. Within the same JVM the UDF will only ever use the first URL used and ignores subsequently changed URLs."
          + "The order of the parameters is the following: nubLookup(api, kingdom, phylum, class_rank, order_rank, family, genus, scientific_name, specific_epithet, infra_specific_epithet, rank)")
  @SqlType(
      "row(responsestatus varchar, usagekey integer, scientificname varchar, rank varchar, status varchar, matchtype varchar, confidence integer,"
          + "kingdomkey integer, phylumkey integer, classkey integer, orderkey integer, familykey integer, genuskey integer, specieskey integer,"
          + "kingdom varchar, phylum varchar, class_ varchar, order_ varchar, family varchar, genus varchar, species varchar)")
  public Block nubLookup(
      @SqlType(StandardTypes.VARCHAR) Slice apiArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice kingdomArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice phylumArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice classRankArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice orderRankArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice familyArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice genusArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice sciNameArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice specificEpithetArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice infraSpecificEpithetArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice rankArg) {
    if (apiArg == null) {
      throw new IllegalArgumentException("Api argument is required");
    }

    String api = apiArg.toStringUtf8();

    String k = clean(kingdomArg);
    String p = clean(phylumArg);
    String c = clean(classRankArg);
    String o = clean(orderRankArg);
    String f = clean(familyArg);
    String g = clean(genusArg);
    String name = clean(sciNameArg);
    String sp = clean(specificEpithetArg);
    String ssp = clean(infraSpecificEpithetArg);
    Rank rank = rankArg != null ? RANK_PARSER.parse(rankArg.toStringUtf8()).getPayload() : null;

    // TODO: add authorship as a standalone parameter
    ParseResult<NameUsageMatch> response =
        getInterpreter(api).match(k, p, c, o, f, g, name, null, null, sp, ssp, rank);

    RowType rowType =
        RowType.rowType(
            new RowType.Field(Optional.of("responsestatus"), VARCHAR),
            new RowType.Field(Optional.of("usagekey"), INTEGER),
            new RowType.Field(Optional.of("scientificname"), VARCHAR),
            new RowType.Field(Optional.of("rank"), VARCHAR),
            new RowType.Field(Optional.of("status"), VARCHAR),
            new RowType.Field(Optional.of("matchtype"), VARCHAR),
            new RowType.Field(Optional.of("confidence"), INTEGER),
            new RowType.Field(Optional.of("kingdomkey"), INTEGER),
            new RowType.Field(Optional.of("phylumkey"), INTEGER),
            new RowType.Field(Optional.of("classkey"), INTEGER),
            new RowType.Field(Optional.of("orderkey"), INTEGER),
            new RowType.Field(Optional.of("familykey"), INTEGER),
            new RowType.Field(Optional.of("genuskey"), INTEGER),
            new RowType.Field(Optional.of("specieskey"), INTEGER),
            new RowType.Field(Optional.of("kingdom"), VARCHAR),
            new RowType.Field(Optional.of("phylum"), VARCHAR),
            new RowType.Field(Optional.of("class_"), VARCHAR),
            new RowType.Field(Optional.of("order_"), VARCHAR),
            new RowType.Field(Optional.of("family"), VARCHAR),
            new RowType.Field(Optional.of("genus"), VARCHAR),
            new RowType.Field(Optional.of("species"), VARCHAR));
    RowBlockBuilder blockBuilder = (RowBlockBuilder) rowType.createBlockBuilder(null, 5);
    SingleRowBlockWriter builder = blockBuilder.beginBlockEntry();

    Consumer<Integer> intWriter =
        v -> {
          if (v != null) {
            INTEGER.writeLong(builder, v);
          } else {
            builder.appendNull();
          }
        };

    Consumer<String> stringWriter =
        v -> {
          if (v != null) {
            VARCHAR.writeString(builder, v);
          } else {
            builder.appendNull();
          }
        };

    if (response != null) {
      stringWriter.accept(response.getStatus().name());

      if (response.getPayload() != null) {
        NameUsageMatch lookup = response.getPayload();
        intWriter.accept(lookup.getUsageKey());
        stringWriter.accept(lookup.getScientificName());
        stringWriter.accept(lookup.getRank().name());
        stringWriter.accept(lookup.getStatus().name());
        stringWriter.accept(lookup.getMatchType().name());
        intWriter.accept(lookup.getConfidence());

        intWriter.accept(lookup.getKingdomKey());
        intWriter.accept(lookup.getPhylumKey());
        intWriter.accept(lookup.getClassKey());
        intWriter.accept(lookup.getOrderKey());
        intWriter.accept(lookup.getFamilyKey());
        intWriter.accept(lookup.getGenusKey());
        intWriter.accept(lookup.getSpeciesKey());

        stringWriter.accept(lookup.getKingdom());
        stringWriter.accept(lookup.getPhylum());
        stringWriter.accept(lookup.getClazz());
        stringWriter.accept(lookup.getOrder());
        stringWriter.accept(lookup.getFamily());
        stringWriter.accept(lookup.getGenus());
        stringWriter.accept(lookup.getSpecies());
      } else if (response.getError() != null) {
        log.error("Error finding species match", response.getError());
      }
    }

    blockBuilder.closeEntry();
    return blockBuilder.build().getObject(0, Block.class);
  }
}
