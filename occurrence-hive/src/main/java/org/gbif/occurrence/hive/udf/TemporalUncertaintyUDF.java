package org.gbif.occurrence.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.gbif.occurrence.cube.functions.TemporalUncertainty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculate the temporal uncertainty of an eventDate and optional eventTime.
 */
@Description(name = "temporalUncertainty",
  value = "_FUNC_(String, String) - Calculate the temporal uncertainty of an eventDate and optional eventTime",
  extended = "Example: eventDate(eventDate, eventTime)")
public class TemporalUncertaintyUDF extends GenericUDF {

  private static final Logger LOG = LoggerFactory.getLogger(TemporalUncertaintyUDF.class.getName());

  private final TemporalUncertainty temporalUncertainty = new TemporalUncertainty();

  private ObjectInspectorConverters.Converter[] converters;

  public String getConvertArguments(int idx, DeferredObject[] arguments) throws HiveException {
    return arguments[idx].get() == null ? null : converters[idx].convert(arguments[idx].get()).toString();
  }

  @Override
  public LongWritable evaluate(DeferredObject[] arguments) throws HiveException {
    String eventDate = getConvertArguments(0, arguments);
    String eventTime = arguments.length == 2 ? getConvertArguments(1, arguments) : null;

    final LongWritable resultString = new LongWritable();

    try {
      resultString.set(temporalUncertainty.fromEventDateAndEventTime(eventDate, eventTime));
      return resultString;
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public String getDisplayString(String[] strings) {
    if (strings.length == 1) {
      return "temporalUncertainty(" + strings[0] + ')';
    } else if (strings.length == 2) {
      return "temporalUncertainty(" + strings[0] + ", " + strings[1] + ')';
    } else {
      throw new AssertionError("Incorrect number of arguments");
    }
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1 && arguments.length != 2) {
      throw new UDFArgumentException("temporalUncertainty takes 1 or 2 arguments");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters
        .getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
  }
}
