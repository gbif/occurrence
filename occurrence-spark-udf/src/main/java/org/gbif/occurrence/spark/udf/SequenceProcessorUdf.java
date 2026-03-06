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
package org.gbif.occurrence.spark.udf;

import org.gbif.sequencing.SequenceProcessor;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF11;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Spark UDF for processing DNA/RNA sequences.
 * Takes a sequence string, optional sequence ID, and optional config parameters.
 * Returns a struct containing the cleaned sequence and various quality metrics.
 *
 * <p>Config parameters are optional - if null, default values from SequenceProcessor.Config.defaultConfig() are used.
 */
public class SequenceProcessorUdf implements UDF11<String, String, String, Integer, String, String, String, String, String, Integer, Integer, Row> {

  private static final SequenceProcessor.Config DEFAULT_CONFIG = SequenceProcessor.Config.defaultConfig();

  /**
   * Returns the schema for the result struct.
   * This should be used when registering the UDF with Spark.
   */
  public static StructType resultSchema() {
    return DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("seqId", DataTypes.StringType, true),
      DataTypes.createStructField("rawSequence", DataTypes.StringType, true),
      DataTypes.createStructField("sequence", DataTypes.StringType, true),
      DataTypes.createStructField("sequenceLength", DataTypes.IntegerType, false),
      DataTypes.createStructField("nonIupacFraction", DataTypes.DoubleType, true),
      DataTypes.createStructField("nonACGTNFraction", DataTypes.DoubleType, true),
      DataTypes.createStructField("nFraction", DataTypes.DoubleType, true),
      DataTypes.createStructField("nNrunsCapped", DataTypes.IntegerType, false),
      DataTypes.createStructField("gcContent", DataTypes.DoubleType, true),
      DataTypes.createStructField("naturalLanguageDetected", DataTypes.BooleanType, false),
      DataTypes.createStructField("endsTrimmed", DataTypes.BooleanType, false),
      DataTypes.createStructField("gapsOrWhitespaceRemoved", DataTypes.BooleanType, false),
      DataTypes.createStructField("nucleotideSequenceID", DataTypes.StringType, true),
      DataTypes.createStructField("invalid", DataTypes.BooleanType, false)
    });
  }

  @Override
  public Row call(
      String sequence,
      String seqId,
      String anchorChars,
      Integer anchorMinrun,
      String anchorStrict,
      String gapRegex,
      String naturalLanguageRegex,
      String iupacRna,
      String iupacDna,
      Integer nrunCapFrom,
      Integer nrunCapTo) throws Exception {
    if (sequence == null) {
      return null;
    }

    // Build config using provided values or defaults
    SequenceProcessor.Config config = new SequenceProcessor.Config(
      anchorChars != null ? anchorChars : DEFAULT_CONFIG.anchorChars(),
      anchorMinrun != null ? anchorMinrun : DEFAULT_CONFIG.anchorMinrun(),
      anchorStrict != null ? anchorStrict : DEFAULT_CONFIG.anchorStrict(),
      gapRegex != null ? gapRegex : DEFAULT_CONFIG.gapRegex(),
      naturalLanguageRegex != null ? naturalLanguageRegex : DEFAULT_CONFIG.naturalLanguageRegex(),
      iupacRna != null ? iupacRna : DEFAULT_CONFIG.iupacRna(),
      iupacDna != null ? iupacDna : DEFAULT_CONFIG.iupacDna(),
      nrunCapFrom != null ? nrunCapFrom : DEFAULT_CONFIG.nrunCapFrom(),
      nrunCapTo != null ? nrunCapTo : DEFAULT_CONFIG.nrunCapTo()
    );

    SequenceProcessor processor = new SequenceProcessor(config);
    SequenceProcessor.Result result = processor.processOneSequence(sequence, seqId);

    return RowFactory.create(
      result.seqId(),
      result.rawSequence(),
      result.sequence(),
      result.sequenceLength(),
      result.nonIupacFraction(),
      result.nonACGTNFraction(),
      result.nFraction(),
      result.nNrunsCapped(),
      result.gcContent(),
      result.naturalLanguageDetected(),
      result.endsTrimmed(),
      result.gapsOrWhitespaceRemoved(),
      result.nucleotideSequenceID(),
      result.invalid()
    );
  }
}
