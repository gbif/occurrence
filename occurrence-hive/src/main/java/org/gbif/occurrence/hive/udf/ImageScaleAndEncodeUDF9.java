package org.gbif.occurrence.hive.udf;

import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.gbif.occurrence.processor.guice.ApiClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * A UDF that uses the GBIF API to retrieve a resized image and byte-encode it.
 */
@Description(name = "resizeAndEncodeImage9", value = "_FUNC_(apiUrl, identifier)")
public class ImageScaleAndEncodeUDF9 extends UDF {
//  private static final int argLength = 2;

//  private ObjectInspectorConverters.Converter[] converters;
  private static final Logger LOG = LoggerFactory.getLogger(ImageScaleAndEncodeUDF9.class);

  private WebResource webResource;
  private Object lock = new Object();

  public WebResource getWebResource(URI apiWs) {
    init(apiWs);
    return webResource;
  }

  private void init(URI apiWs) {
    if (webResource == null) {
      synchronized (lock) {    // while we were waiting for the lock, another thread may have instantiated the object
        if (webResource == null) {
          LOG.info("Create new web resource using API at {}", apiWs);
          ApiClientConfiguration cfg = new ApiClientConfiguration();
          cfg.url = apiWs;
          webResource = cfg.newApiClient();
        }
      }
    }
  }

  private final BytesWritable data = new BytesWritable();

  public BytesWritable evaluate(Text api, Text identifier) {
    URI wsApi = URI.create(api.toString());

    byte[] imageBytes = null;

    if (identifier != null) {
//      imageBytes = getWebResource(wsApi).path("image/unsafe/fit-in/320x320/filters:format(jpg)/"+identifier.toString()).getRequestBuilder().get(byte[].class);
      try {
        imageBytes = getWebResource(wsApi).path("image/unsafe/64x64/filters:format(jpg)/" + identifier.toString()).getRequestBuilder().get(byte[].class);
      } catch (Exception e) {}
    }

    return new BytesWritable(imageBytes);
  }

//  @Override
//  public Object evaluate(DeferredObject[] arguments) throws HiveException {
//    assert arguments.length == argLength;
//
//    URI api = URI.create(arguments[0].get().toString());
//    URI identifierUrl = URI.create(arguments[1].get().toString());
//
//    byte[] imageBytes = null;
//
//    if (identifierUrl != null) {
//      imageBytes = getWebResource(api).path("image/unsafe/fit-in/322x320/filters:format(jpg)/"+arguments[1].get().toString()).getRequestBuilder().get(byte[].class);
//    }
//
//    return imageBytes;
//  }

//  @Override
//  public String getDisplayString(String[] strings) {
//    assert strings.length == 2;
//    return "resizeAndEncodeImage(" + strings[0] + ", " + strings[1] + ')';
//  }
//
//  @Override
//  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
//    if (arguments.length != 2) {
//      throw new UDFArgumentException("resizeAndEncodeImage takes two arguments");
//    }
//
//    converters = new ObjectInspectorConverters.Converter[arguments.length];
//    for (int i = 0; i < arguments.length; i++) {
//      converters[i] = ObjectInspectorConverters
//        .getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableStringObjectInspector);
//    }
//
//    return ObjectInspectorFactory
//      .getStandardStructObjectInspector(Arrays.asList("identifier"), Arrays
//        .asList(PrimitiveObjectInspectorFactory.writableStringObjectInspector));
//  }
}
