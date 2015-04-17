package org.gbif.occurrence.hive.udf;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.occurrence.processor.guice.ApiClientConfiguration;
import org.gbif.occurrence.processor.interpreting.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.LocationInterpreter;
import org.gbif.occurrence.processor.interpreting.result.CoordinateResult;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A UDF that uses the GBIF API to verify coordinates look sensible.
 * If coordinates aren't available then country is interpreted only.
 * If coordinates are present, then country and coordinates are returned only if they don't contradict, otherwise they
 * are BOTH dropped.
 * Note: This is used for the GBIF EU BON analysis.
 */
@Description(name = "parseCoordinates", value = "_FUNC_(apiUrl, latitude, longitude, verbatim_country)")
public class CoordinateCountryParseUDF extends GenericUDF {
  private static final int argLength = 4;

  private ObjectInspectorConverters.Converter[] converters;
  private static final Logger LOG = LoggerFactory.getLogger(CoordinateCountryParseUDF.class);

  private LocationInterpreter locInterpreter;
  private CoordinateInterpreter coordInterpreter;
  private Object lock = new Object();

  public LocationInterpreter getLocInterpreter(URI apiWs) {
    init(apiWs);
    return locInterpreter;
  }

  public CoordinateInterpreter getCoordInterpreter(URI apiWs) {
    init(apiWs);
    return coordInterpreter;
  }

  private void init(URI apiWs) {
    if(locInterpreter == null) {
      synchronized(lock) {    // while we were waiting for the lock, another thread may have instantiated the object
        if (locInterpreter == null) {
          LOG.info("Create new coordinate & location interpreter using API at {}", apiWs);
          ApiClientConfiguration cfg = new ApiClientConfiguration();
          cfg.url = apiWs;
          coordInterpreter = new CoordinateInterpreter(cfg.newApiClient());
          locInterpreter = new LocationInterpreter(coordInterpreter);
        }
      }
    }
  }
  
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert arguments.length == argLength;

    URI api = URI.create(arguments[0].get().toString().toUpperCase());

    // Interpret the country to pass in to the geo lookup
    String country = arguments[3].get() == null ? null : converters[3].convert(arguments[3].get()).toString();
    Country interpretedCountry = Country.UNKNOWN;
    if (country != null) {
      ParseResult<Country> r = getLocInterpreter(api).interpretCountry(country);
      if (r.isSuccessful() && r.getPayload() != null) {
        interpretedCountry = r.getPayload();
      }
    }
    String iso = Country.UNKNOWN == interpretedCountry ? null : interpretedCountry.getIso2LetterCode();

    List<Object> result = Lists.newArrayList(3);

    if (arguments[1].get() == null || arguments[2].get() == null
      || Strings.isNullOrEmpty(arguments[1].get().toString()) || Strings.isNullOrEmpty(arguments[2].get().toString())) {
      result.add(null);
      result.add(null);
      result.add(iso); // no coords to dispute the iso
      return result;
    }

    String latitude = converters[0].convert(arguments[1].get()).toString();
    String longitude = converters[1].convert(arguments[2].get()).toString();

    // while we have interpreted the country to try and pass something sensible to the CoordinateInterpreter,
    // it will not infer countries if we pass in UNKNOWN, as it likes NULL.
    interpretedCountry = Country.UNKNOWN == interpretedCountry ? null : interpretedCountry;

    // LOG.info("Parsing lat[{}], lng[{}], country[{}]", latitude, longitude, interpretedCountry);
    OccurrenceParseResult<CoordinateResult> response = getCoordInterpreter(api)
      .interpretCoordinate(latitude, longitude, null, interpretedCountry);

    if (response != null && response.isSuccessful() && !hasSpatialIssue(response.getIssues())) {
      CoordinateResult cc = response.getPayload();
      // we use the result, which often includes interpreted countries
      if (cc.getCountry() == null || Country.UNKNOWN == cc.getCountry()) {
        iso = null;
      } else {
        iso = cc.getCountry().getIso2LetterCode();
      }

      result.add(cc.getLatitude());
      result.add(cc.getLongitude());
      result.add(iso);

    } else {
      result.add(null);
      result.add(null);
      result.add(null);
    }
    return result;
  }

  private static boolean hasSpatialIssue(Collection<OccurrenceIssue> issues) {
    for (OccurrenceIssue rule : OccurrenceIssue.GEOSPATIAL_RULES) {
      if (issues.contains(rule)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getDisplayString(String[] strings) {
    assert strings.length == 3;
    return "parseCoordinates(" + strings[0] + ", " + strings[1] + ", " + strings[2] + ")";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 3) {
      throw new UDFArgumentException("parseCoordinates takes three arguments");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters
        .getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return ObjectInspectorFactory
      .getStandardStructObjectInspector(Arrays.asList("latitude", "longitude", "country"), Arrays
        .<ObjectInspector>asList(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
          PrimitiveObjectInspectorFactory.javaStringObjectInspector));
  }
}
