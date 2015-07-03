package org.gbif.occurrence.hbaseindexer;

import org.gbif.occurrence.common.json.MediaSerDeserUtils;

import java.util.Collection;

import com.google.common.collect.ImmutableList;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;

public class MediaTypeByteArrayMapper implements ByteArrayValueMapper {

  @Override
  public Collection<Object> map(byte[] input) {
    try {
      return ImmutableList.of((Object) MediaSerDeserUtils.extractMediaTypes(input));
    } catch (IllegalArgumentException e) {
      return ImmutableList.of();
    }
  }

}
