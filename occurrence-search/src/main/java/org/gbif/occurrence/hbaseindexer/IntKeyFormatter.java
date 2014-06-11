package org.gbif.occurrence.hbaseindexer;

//public class IntKeyFormatter extends BaseUniqueKeyFormatter implements UniqueKeyFormatter {
//
//  @Override
//  protected String encodeAsString(byte[] bytes) {
//    return new Integer(Ints.fromByteArray(bytes)).toString();
//  }
//
//  @Override
//  protected byte[] decodeFromString(String value) {
//    try {
//      return Ints.toByteArray(Ints.fromByteArray(Hex.decodeHex(value.toCharArray())));
//    } catch (DecoderException e) {
//      throw new IllegalArgumentException("Value '" + value + "' can't be decoded as hex", e);
//    }
//  }
//}
