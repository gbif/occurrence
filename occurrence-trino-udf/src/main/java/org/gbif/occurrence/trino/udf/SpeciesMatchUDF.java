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

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.RankParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.occurrence.trino.processor.conf.ApiClientConfiguration;
import org.gbif.occurrence.trino.processor.interpreters.TaxonomyInterpreter;

import java.util.Optional;
import java.util.function.BiConsumer;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import lombok.extern.slf4j.Slf4j;

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

    try {

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

      RowBlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 5);
      rowBlockBuilder.buildEntry(
          builder -> {
            BiConsumer<BlockBuilder, Integer> intWriter =
                (blockBuilder, v) -> {
                  if (v != null) {
                    INTEGER.writeLong(blockBuilder, v);
                  } else {
                    blockBuilder.appendNull();
                  }
                };

            BiConsumer<BlockBuilder, String> stringWriter =
                (blockBuilder, v) -> {
                  if (v != null) {
                    VARCHAR.writeString(blockBuilder, v);
                  } else {
                    blockBuilder.appendNull();
                  }
                };

            BiConsumer<BlockBuilder, Enum> enumWriter =
                (blockBuilder, v) -> {
                  if (v != null) {
                    VARCHAR.writeString(blockBuilder, v.name());
                  } else {
                    blockBuilder.appendNull();
                  }
                };

            if (response != null) {
              stringWriter.accept(builder.get(0), response.getStatus().name());

              if (response.getPayload() != null) {
                NameUsageMatch lookup = response.getPayload();
                intWriter.accept(builder.get(1), lookup.getUsageKey());
                stringWriter.accept(builder.get(2), lookup.getScientificName());
                enumWriter.accept(builder.get(3), lookup.getRank());
                enumWriter.accept(builder.get(4), lookup.getStatus());
                enumWriter.accept(builder.get(5), lookup.getMatchType());
                intWriter.accept(builder.get(6), lookup.getConfidence());

                intWriter.accept(builder.get(7), lookup.getKingdomKey());
                intWriter.accept(builder.get(8), lookup.getPhylumKey());
                intWriter.accept(builder.get(9), lookup.getClassKey());
                intWriter.accept(builder.get(10), lookup.getOrderKey());
                intWriter.accept(builder.get(11), lookup.getFamilyKey());
                intWriter.accept(builder.get(12), lookup.getGenusKey());
                intWriter.accept(builder.get(13), lookup.getSpeciesKey());

                stringWriter.accept(builder.get(14), lookup.getKingdom());
                stringWriter.accept(builder.get(15), lookup.getPhylum());
                stringWriter.accept(builder.get(16), lookup.getClazz());
                stringWriter.accept(builder.get(17), lookup.getOrder());
                stringWriter.accept(builder.get(18), lookup.getFamily());
                stringWriter.accept(builder.get(19), lookup.getGenus());
                stringWriter.accept(builder.get(20), lookup.getSpecies());
              } else {
                if (response.getError() != null) {
                  log.error("Error finding species match", response.getError());
                }
                // set all fields to null
                for (int i = 1; i < 21; i++) {
                  builder.get(i).appendNull();
                }
              }
            }
          });
      return rowBlockBuilder.build().getObject(0, Block.class);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    }

    // if we got till here we return an empty row
    RowBlockBuilder blockBuilder = rowType.createBlockBuilder(null, 5);
    blockBuilder.buildEntry(
        b -> {
          // set all fields to null
          for (int i = 0; i < 21; i++) {
            b.get(i).appendNull();
          }
        });

    return blockBuilder.build().getObject(0, Block.class);
  }
}
