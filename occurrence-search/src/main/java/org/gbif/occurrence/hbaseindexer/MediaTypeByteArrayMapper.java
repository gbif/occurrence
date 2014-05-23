package org.gbif.occurrence.hbaseindexer;

import org.gbif.occurrence.common.json.MediaSerDeserUtils;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

import com.google.common.collect.ImmutableList;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class MediaTypeByteArrayMapper implements ByteArrayValueMapper {

  private static final Log log = LogFactory.getLog(MediaTypeByteArrayMapper.class);

  @Override
  public Collection<Object> map(byte[] input) {
    try {
      return ImmutableList.of((Object) MediaSerDeserUtils.extractMediaTypes(input));
    } catch (IllegalArgumentException e) {
      log.warn(
        String.format("Error mapping media types from %s", new String(input, StandardCharsets.UTF_8), e));
      return ImmutableList.of();
    }
  }

}
