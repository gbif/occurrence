package org.gbif.occurrence.hbaseindexer;

import org.gbif.occurrence.common.json.MediaSerDeserUtils;

import java.util.Collection;

import com.google.common.collect.ImmutableList;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;

/**
 * Extracts a list of  media types from a  media object.
 */
public class MediaTypeByteArrayMapper implements ByteArrayValueMapper {

  @Override
  public Collection<String> map(byte[] input) {
    try {
      return ImmutableList.copyOf(MediaSerDeserUtils.extractMediaTypes(input));
    } catch (IllegalArgumentException e) {
      return ImmutableList.of();
    }
  }

}
