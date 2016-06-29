package org.gbif.occurrence.hive.udf;

import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.date.AtomizedLocalDate;
import org.gbif.occurrence.processor.interpreting.TemporalInterpreter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.threeten.bp.temporal.TemporalAccessor;

/**
 * Parses year month and day only.
 */
@Description(
  name = "parseDate",
  value = "_FUNC_(year, month, day, event_date)")
public class DateParseUDF extends GenericUDF {

  private ObjectInspectorConverters.Converter[] converters;

  @Override
  public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
    assert arguments.length == 4;

    String year = getArgument(0, arguments);
    String month = getArgument(1, arguments);
    String day = getArgument(2, arguments);
    String event_date = getArgument(3, arguments);
    List<Object> result = new ArrayList<Object>(4);
    try {
      OccurrenceParseResult<AtomizedLocalDate> parsed =
        TemporalInterpreter.interpretEventDate(year, month, day, event_date);
      OccurrenceParseResult<TemporalAccessor> parsed2 =
              TemporalInterpreter.interpretRecordedDate(year, month, day, event_date);
      if (parsed.isSuccessful() && parsed.getIssues().isEmpty()) {
        result.add(parsed.getPayload().getYear());
        result.add(parsed.getPayload().getMonth());
        result.add(parsed.getPayload().getDay());
        result.add(TemporalInterpreter.toUTCDate(parsed2.getPayload()).getTime());
      }
      else{
        result.add(null);
        result.add(null);
        result.add(null);
        result.add(null);
      }
    } catch (Exception e) {
      // not much to do - indicates bad data
    }

    return result;
  }

  private String getArgument(int index, GenericUDF.DeferredObject[] arguments) throws HiveException {
    DeferredObject deferredObject = arguments[index];
    if (deferredObject == null) {
      return null;
    }

    Object convertedObject = converters[index].convert(deferredObject.get());
    return convertedObject == null ? null : convertedObject.toString();
  }

  @Override
  public String getDisplayString(String[] strings) {
    assert strings.length == 4;
    return "parseDate(" + strings[0] + ", " + strings[1] + ", " + strings[2] + ", " + strings[3] + ')';
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 4) {
      throw new UDFArgumentException("parseDate takes four arguments");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters
        .getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList("year", "month", "day", "epoch"), Arrays
        .<ObjectInspector>asList(
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaLongObjectInspector));
  }

}
