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
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.geocode.ws.service.impl.ShapefileGeocoder;
import org.gbif.occurrence.trino.processor.interpreters.CoordinateInterpreter;
import org.gbif.occurrence.trino.processor.interpreters.LocationInterpreter;
import org.gbif.occurrence.trino.processor.result.CoordinateResult;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * A UDF that uses the GBIF geocoder shapefiles to verify coordinates look sensible. If coordinates
 * aren't available then country is interpreted only. If coordinates are present, then country and
 * coordinates are returned only if they don't contradict, otherwise they are BOTH dropped. Note:
 * This is used for the quarterly analytics and GBIF EU BON analysis.
 */
@Slf4j
public class CoordinateCountryParseUDF {
  private ShapefileGeocoder shapefileGeocoder;
  private LocationInterpreter locInterpreter;
  private CoordinateInterpreter coordInterpreter;
  private Object lock = new Object();

  public LocationInterpreter getLocInterpreter(String layersPath) {
    init(layersPath);
    return locInterpreter;
  }

  public CoordinateInterpreter getCoordInterpreter(String layersPath) {
    init(layersPath);
    return coordInterpreter;
  }

  private void init(String layersPath) {
    if (locInterpreter == null) {
      synchronized (
          lock) { // while we were waiting for the lock, another thread may have instantiated the
        // object
        if (locInterpreter == null) {
          log.info(
              "Create new coordinate & location interpreter using in-process shapefile geocoder with shapefiles at {}",
              layersPath);
          shapefileGeocoder =
              new ShapefileGeocoder(layersPath, Collections.singletonList("PoliticalLayer"));
          coordInterpreter = new CoordinateInterpreter(shapefileGeocoder);
          locInterpreter = new LocationInterpreter(coordInterpreter);
        }
      }
    }
  }

  @ScalarFunction(value = "parseGeo", deterministic = true)
  @Description(
      "A UDF that uses the GBIF geocoder shapefiles to verify coordinates look sensible. "
          + "If coordinates aren't available then country is interpreted only. "
          + "If coordinates are present, then country and coordinates are returned only if they don't contradict, otherwise they are BOTH dropped."
          + "The order of the parameters is the following: parseGeo(layersPath, latitude, longitude, verbatim_country).")
  @SqlType("row(latitude double, longitude double, country varchar)")
  public Block parseGeo(
      @SqlType(StandardTypes.VARCHAR) Slice layersPathArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice latitudeArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice longitudeArg,
      @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice countryArg) {
    if (layersPathArg == null) {
      throw new IllegalArgumentException("Layers path argument is required");
    }

    String layersPath = layersPathArg.toStringUtf8();

    // Interpret the country to pass in to the geo lookup
    String country = countryArg == null ? null : countryArg.toStringUtf8();
    Country interpretedCountry = Country.UNKNOWN;
    if (country != null) {
      ParseResult<Country> r = getLocInterpreter(layersPath).interpretCountry(country);
      if (r.isSuccessful() && r.getPayload() != null) {
        interpretedCountry = r.getPayload();
      }
    }

    String parsedCountry =
        (Country.UNKNOWN == interpretedCountry
                || Country.INTERNATIONAL_WATERS == interpretedCountry)
            ? null
            : interpretedCountry.getIso2LetterCode();

    Double parsedLatitude = null;
    Double parsedLongitude = null;
    if (latitudeArg != null && longitudeArg != null) {
      String latitude = latitudeArg.toStringUtf8();
      String longitude = longitudeArg.toStringUtf8();

      // while we have interpreted the country to try and pass something sensible to the
      // CoordinateInterpreter,
      // it will not infer countries if we pass in UNKNOWN, as it likes NULL.
      interpretedCountry = Country.UNKNOWN == interpretedCountry ? null : interpretedCountry;

      OccurrenceParseResult<CoordinateResult> response =
          getCoordInterpreter(layersPath)
              .interpretCoordinate(latitude, longitude, null, interpretedCountry);

      if (response != null && response.isSuccessful() && !hasSpatialIssue(response.getIssues())) {
        CoordinateResult cc = response.getPayload();
        // we use the result, which often includes interpreted countries
        if (cc.getCountry() == null || Country.UNKNOWN == cc.getCountry()) {
          parsedCountry = null;
        } else {
          parsedCountry = cc.getCountry().getIso2LetterCode();
        }

        parsedLatitude = cc.getLatitude();
        parsedLongitude = cc.getLongitude();
      }
    }

    RowType rowType =
        RowType.rowType(
            new RowType.Field(Optional.of("latitude"), DOUBLE),
            new RowType.Field(Optional.of("longitude"), DOUBLE),
            new RowType.Field(Optional.of("country"), VARCHAR));
    RowBlockBuilder blockBuilder = (RowBlockBuilder) rowType.createBlockBuilder(null, 3);
    SingleRowBlockWriter builder = blockBuilder.beginBlockEntry();

    if (parsedLatitude != null) {
      DOUBLE.writeDouble(builder, parsedLatitude);
    } else {
      builder.appendNull();
    }
    if (parsedLongitude != null) {
      DOUBLE.writeDouble(builder, parsedLongitude);
    } else {
      builder.appendNull();
    }
    if (parsedCountry != null) {
      VARCHAR.writeString(builder, parsedCountry);
    } else {
      builder.appendNull();
    }

    blockBuilder.closeEntry();
    return blockBuilder.build().getObject(0, Block.class);
  }

  private static boolean hasSpatialIssue(Collection<OccurrenceIssue> issues) {
    for (OccurrenceIssue rule : OccurrenceIssue.GEOSPATIAL_RULES) {
      if (issues.contains(rule)) {
        return true;
      }
    }
    return false;
  }
}
