package org.gbif.occurrence.hive.udf;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.processor.guice.ApiClientConfiguration;
import org.gbif.occurrence.processor.interpreting.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.LocationInterpreter;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.internal.Lists;
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
 *
 * Reinterpret location (latitude, longitude, country) based on verbatim fields.
 * This is used to test addition or changes to location interpretation algorithm.
 *
 *
 */
@Description(name = "reinterpretLocation", value = "_FUNC_(apiUrl, decimalLatitude, decimalLongitude, verbatimLatitude, verbatimLongitude, verbatimCoordinates, geodeticDatum, country, countrycode)")
public class ReinterpretLocationUDF extends GenericUDF {
  private static final int argLength = 9;

  private ObjectInspectorConverters.Converter[] converters;
  private static final Logger LOG = LoggerFactory.getLogger(ReinterpretLocationUDF.class);

  private LocationInterpreter locInterpreter;
  private CoordinateInterpreter coordInterpreter;
  private Object lock = new Object();

  public LocationInterpreter getLocInterpreter(URI apiWs) {
    init(apiWs);
    return locInterpreter;
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

  public String getConvertArguments(int idx, DeferredObject[] arguments) throws HiveException {
    return arguments[idx].get() == null ? null : converters[idx].convert(arguments[idx].get()).toString();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert arguments.length == argLength;

    List<Object> result = Lists.newArrayList(1);
    URI api = URI.create(arguments[0].get().toString());

    String latitude = getConvertArguments(1, arguments);
    String longitude = getConvertArguments(2, arguments);
    String verbatimLatitude = getConvertArguments(3, arguments);
    String verbatimlLongitude = getConvertArguments(4, arguments);
    String verbatimCoordinates = getConvertArguments(5, arguments);
    String geodeticDatum = getConvertArguments(6, arguments);
    String country = getConvertArguments(7, arguments);
    String countryCode = getConvertArguments(8, arguments);

    VerbatimOccurrence verbatim = new VerbatimOccurrence();
    verbatim.setVerbatimField(DwcTerm.decimalLatitude, latitude);
    verbatim.setVerbatimField(DwcTerm.decimalLongitude, longitude);
    verbatim.setVerbatimField(DwcTerm.verbatimLatitude, verbatimLatitude);
    verbatim.setVerbatimField(DwcTerm.verbatimLongitude, verbatimlLongitude);
    verbatim.setVerbatimField(DwcTerm.verbatimCoordinates, verbatimCoordinates);
    verbatim.setVerbatimField(DwcTerm.geodeticDatum, geodeticDatum);
    verbatim.setVerbatimField(DwcTerm.country, country);
    verbatim.setVerbatimField(DwcTerm.countryCode, countryCode);

    Occurrence occ = new Occurrence(verbatim);

    try {
      getLocInterpreter(api).interpretLocation(verbatim, occ);
    }
    catch (Exception e){
      //From VerbatimOccurrenceInterpreter: these interpreters throw a variety of runtime exceptions but should throw checked exceptions
    }

    result.add(occ.getDecimalLatitude());
    result.add(occ.getDecimalLongitude());

    if(occ.getCountry() != null){
      result.add(occ.getCountry().getIso2LetterCode());
    }
    else{
      result.add(null);
    }
    return result;
  }

  @Override
  public String getDisplayString(String[] strings) {
    assert strings.length == argLength;
    return "reinterpretLocation(" + strings[0] + ", " + strings[1] + ", " + strings[2] + ", " + strings[3] +
            ", " + strings[4] + ", " + strings[5] + ", " + strings[6] + ", " + strings[7] + ", " + strings[8] + ')';
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != argLength) {
      throw new UDFArgumentException("compareLocationInterpretation takes 9 arguments");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters
        .getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return ObjectInspectorFactory
      .getStandardStructObjectInspector(Arrays.asList("decimallatitude", "decimallongitude", "countrycode"), Arrays
        .<ObjectInspector>asList(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector, PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector));
  }
}
