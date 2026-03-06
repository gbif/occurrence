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

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SequenceProcessorUdfTest {

  private final SequenceProcessorUdf udf = new SequenceProcessorUdf();

  @Test
  public void testNullSequenceReturnsNull() throws Exception {
    Row result = udf.call(null, null, null, null, null, null, null, null, null, null, null);
    assertNull(result);
  }

  @Test
  public void testBasicSequenceWithDefaults() throws Exception {
    String sequence = "ACGTACGTACGT";
    Row result = udf.call(sequence, "seq1", null, null, null, null, null, null, null, null, null);

    assertNotNull(result);
    assertEquals("seq1", result.getString(0)); // seqId
    assertEquals(sequence, result.getString(1)); // rawSequence
    assertEquals(sequence, result.getString(2)); // sequence (cleaned)
    assertEquals(12, result.getInt(3)); // sequenceLength
    assertEquals(0.0, result.getDouble(4)); // nonIupacFraction
    assertEquals(0.0, result.getDouble(5)); // nonACGTNFraction
    assertFalse(result.getBoolean(9)); // naturalLanguageDetected
    assertFalse(result.getBoolean(10)); // endsTrimmed
    assertFalse(result.getBoolean(11)); // gapsOrWhitespaceRemoved
    assertNotNull(result.getString(12)); // nucleotideSequenceID (MD5)
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testSequenceWithWhitespace() throws Exception {
    String sequence = "ACGT ACGT ACGT";
    Row result = udf.call(sequence, null, null, null, null, null, null, null, null, null, null);

    assertNotNull(result);
    assertEquals("ACGTACGTACGT", result.getString(2)); // sequence (cleaned)
    assertTrue(result.getBoolean(11)); // gapsOrWhitespaceRemoved
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testSequenceWithGaps() throws Exception {
    String sequence = "ACGT-ACGT..ACGT";
    Row result = udf.call(sequence, null, null, null, null, null, null, null, null, null, null);

    assertNotNull(result);
    assertEquals("ACGTACGTACGT", result.getString(2)); // sequence (cleaned)
    assertTrue(result.getBoolean(11)); // gapsOrWhitespaceRemoved
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testRnaToDonaConversion() throws Exception {
    String sequence = "ACGUACGUACGU";
    Row result = udf.call(sequence, null, null, null, null, null, null, null, null, null, null);

    assertNotNull(result);
    assertEquals("ACGTACGTACGT", result.getString(2)); // U converted to T
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testLowercaseToUppercase() throws Exception {
    String sequence = "acgtacgtacgt";
    Row result = udf.call(sequence, null, null, null, null, null, null, null, null, null, null);

    assertNotNull(result);
    assertEquals("ACGTACGTACGT", result.getString(2)); // sequence uppercased
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testNaturalLanguageDetection() throws Exception {
    // Default naturalLanguageRegex is "UNMERGED"
    String sequence = "ACGTUNMERGEDACGT";
    Row result = udf.call(sequence, null, null, null, null, null, null, null, null, null, null);

    assertNotNull(result);
    assertTrue(result.getBoolean(9)); // naturalLanguageDetected
    assertTrue(result.getBoolean(13)); // invalid
  }

  @Test
  public void testNrunCapping() throws Exception {
    // Default nrunCapFrom=6, nrunCapTo=5, meaning N-runs of 6+ are capped to 5
    String sequence = "ACGTACGTNNNNNNNNNNACGTACGT";
    Row result = udf.call(sequence, null, null, null, null, null, null, null, null, null, null);

    assertNotNull(result);
    String cleanedSequence = result.getString(2);
    // N-run of 10 should be capped to 5
    assertTrue(cleanedSequence.contains("NNNNN"));
    assertFalse(cleanedSequence.contains("NNNNNN")); // Should not have 6 consecutive Ns
    assertEquals(1, result.getInt(7)); // nNrunsCapped
    assertFalse(result.getBoolean(13)); // invalid
  }

  @Test
  public void testCustomAnchorChars() throws Exception {
    // Custom anchorChars - only recognize A and C as valid anchors
    String sequence = "XXXXACACACACACXXXXACACACACACXXXX";
    Row result = udf.call(sequence, null, "AC", 8, null, null, null, null, null, null, null);

    assertNotNull(result);
    assertTrue(result.getBoolean(10)); // endsTrimmed
  }

  @Test
  public void testCustomNrunCap() throws Exception {
    // Custom nrunCapFrom=4, nrunCapTo=2
    String sequence = "ACGTACGTNNNNACGTACGT";
    Row result = udf.call(sequence, null, null, null, null, null, null, null, null, 4, 2);

    assertNotNull(result);
    String cleanedSequence = result.getString(2);
    // N-run of 4 should be capped to 2
    assertTrue(cleanedSequence.contains("NN"));
    assertFalse(cleanedSequence.contains("NNNN")); // Should not have 4 consecutive Ns
    assertEquals(1, result.getInt(7)); // nNrunsCapped
  }

  @Test
  public void testCustomGapRegex() throws Exception {
    // Custom gapRegex to also remove underscores
    String sequence = "ACGT_ACGT_ACGT";
    Row result = udf.call(sequence, null, null, null, null, "[-\\._]", null, null, null, null, null);

    assertNotNull(result);
    assertEquals("ACGTACGTACGT", result.getString(2)); // underscores removed
    assertTrue(result.getBoolean(11)); // gapsOrWhitespaceRemoved
  }

  @Test
  public void testCustomNaturalLanguageRegex() throws Exception {
    // Custom naturalLanguageRegex to detect "INVALID"
    String sequence = "ACGTINVALIDACGT";
    Row result = udf.call(sequence, null, null, null, null, null, "INVALID", null, null, null, null);

    assertNotNull(result);
    assertTrue(result.getBoolean(9)); // naturalLanguageDetected
    assertTrue(result.getBoolean(13)); // invalid
  }

  @Test
  public void testGcContent() throws Exception {
    // Sequence with known GC content: 6 GC out of 12 ACGT = 50%
    String sequence = "GCGCGCATATAT";
    Row result = udf.call(sequence, null, null, null, null, null, null, null, null, null, null);

    assertNotNull(result);
    assertEquals(0.5, result.getDouble(8), 0.001); // gcContent = 50%
  }

  @Test
  public void testNonIupacCharactersMarkInvalid() throws Exception {
    // Sequence with non-IUPAC characters (X is not in default IUPAC DNA)
    // Need 8+ consecutive anchor chars for the sequence not to be wiped
    String sequence = "ACGTACGTACGTXACGTACGTACGT";
    Row result = udf.call(sequence, null, null, null, null, null, null, null, null, null, null);

    assertNotNull(result);
    assertTrue(result.getDouble(4) > 0); // nonIupacFraction > 0
    assertTrue(result.getBoolean(13)); // invalid
    assertNull(result.getString(2)); // sequence is null when invalid
    assertNull(result.getString(12)); // nucleotideSequenceID is null when invalid
  }

  @Test
  public void testResultSchemaNotNull() {
    assertNotNull(SequenceProcessorUdf.resultSchema());
    assertEquals(14, SequenceProcessorUdf.resultSchema().fields().length);
  }

  @Test
  public void testEmptySequence() throws Exception {
    String sequence = "";
    Row result = udf.call(sequence, null, null, null, null, null, null, null, null, null, null);

    assertNotNull(result);
    assertEquals(0, result.getInt(3)); // sequenceLength
  }

  @Test
  public void testQuestionMarkToN() throws Exception {
    // Need 8+ consecutive anchor chars for the sequence not to be wiped
    String sequence = "ACGTACGTACGT?ACGTACGTACGT";
    Row result = udf.call(sequence, null, null, null, null, null, null, null, null, null, null);

    assertNotNull(result);
    assertTrue(result.getString(2).contains("N")); // ? converted to N
    assertFalse(result.getString(2).contains("?")); // No ? in result
  }

  @Test
  public void testAllConfigParamsProvided() throws Exception {
    String sequence = "acgt-acgt";
    Row result = udf.call(
      sequence,
      "testId",
      "ACGTU",        // anchorChars
      4,              // anchorMinrun
      "ACGTU",        // anchorStrict
      "[-]",          // gapRegex
      "NOTFOUND",     // naturalLanguageRegex
      "ACGTURYSWKMBDHVN", // iupacRna
      "ACGTRYSWKMBDHVN",  // iupacDna
      6,              // nrunCapFrom
      5               // nrunCapTo
    );

    assertNotNull(result);
    assertEquals("testId", result.getString(0));
    assertEquals("ACGTACGT", result.getString(2)); // cleaned sequence
    assertFalse(result.getBoolean(13)); // invalid
  }
}

