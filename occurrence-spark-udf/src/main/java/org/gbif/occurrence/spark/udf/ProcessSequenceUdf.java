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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.Arrays;

/**
 * Hive-compatible GenericUDF for processing DNA/RNA sequences with a single parameter.
 * Takes a sequence string and returns a struct containing the cleaned sequence and various quality metrics.
 * Uses default configuration for all processing options.
 *
 * <p>Usage in SQL:
 * <pre>
 * CREATE FUNCTION processSequence AS 'org.gbif.occurrence.spark.udf.ProcessSequenceUdf';
 * SELECT processSequence(dnasequence).sequence AS cleansequence FROM table;
 * </pre>
 */
public class ProcessSequenceUdf extends GenericUDF {

  private static final SequenceProcessor.Config DEFAULT_CONFIG = SequenceProcessor.Config.defaultConfig();

  private transient StringObjectInspector inputInspector;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("processSequence() requires exactly 1 argument: sequence string");
    }

    if (!(arguments[0] instanceof StringObjectInspector)) {
      throw new UDFArgumentException("processSequence() argument must be a string");
    }

    inputInspector = (StringObjectInspector) arguments[0];

    // Return a struct with all the result fields
    return ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList(
        "seqId", "rawSequence", "sequence", "sequenceLength",
        "nonIupacFraction", "nonACGTNFraction", "nFraction", "nNrunsCapped",
        "gcContent", "naturalLanguageDetected", "endsTrimmed", "gapsOrWhitespaceRemoved",
        "nucleotideSequenceID", "invalid"
      ),
      Arrays.asList(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,    // seqId
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,    // rawSequence
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,    // sequence
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,       // sequenceLength
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,    // nonIupacFraction
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,    // nonACGTNFraction
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,    // nFraction
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,       // nNrunsCapped
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,    // gcContent
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,   // naturalLanguageDetected
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,   // endsTrimmed
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,   // gapsOrWhitespaceRemoved
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,    // nucleotideSequenceID
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector    // invalid
      )
    );
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0] == null || arguments[0].get() == null) {
      return null;
    }

    String sequence = inputInspector.getPrimitiveJavaObject(arguments[0].get());
    if (sequence == null) {
      return null;
    }

    SequenceProcessor processor = new SequenceProcessor(DEFAULT_CONFIG);
    SequenceProcessor.Result result = processor.processOneSequence(sequence, null);

    return new Object[] {
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
    };
  }

  @Override
  public String getDisplayString(String[] children) {
    return "processSequence(" + (children.length > 0 ? children[0] : "") + ")";
  }
}

