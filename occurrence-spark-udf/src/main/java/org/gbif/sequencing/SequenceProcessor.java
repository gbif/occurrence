package org.gbif.sequencing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DNA/RNA sequence cleaning processor.
 * Processes raw sequences through a multi-stage pipeline to produce cleaned sequences with metrics.
 */
public class SequenceProcessor {

  /**
   * Configuration for sequence processing
   */
  public record Config(
    String anchorChars,
    int anchorMinrun,
    String anchorStrict,
    String gapRegex,
    String naturalLanguageRegex,
    String iupacRna,
    String iupacDna,
    int nrunCapFrom,
    int nrunCapTo
  ) {

    /**
     * Load configuration from the default config.yaml file
     */
    public static Config defaultConfig() {
      return new Config(
        "ACGTU",
        8,
        "ACGTU",
        "[-\\.]",
        "UNMERGED",
        "ACGTURYSWKMBDHVN",
        "ACGTRYSWKMBDHVN",
        6,
        5
      );
    }
  }


  /**
   * Result of sequence processing containing cleaned sequence and metrics
   */
  public record Result(
    String seqId,
    String rawSequence,
    String sequence,
    int sequenceLength,
    Double nonIupacFraction,
    Double nonACGTNFraction,
    Double nFraction,
    int nNrunsCapped,
    Double gcContent,
    boolean naturalLanguageDetected,
    boolean endsTrimmed,
    boolean gapsOrWhitespaceRemoved,
    String nucleotideSequenceID,
    boolean invalid
  ) {}

  /**
   * Helper record for trim operations
   */
  private record TrimResult(String s, boolean did) {}

  private final Config config;

  public SequenceProcessor() {
    this(Config.defaultConfig());
  }

  public SequenceProcessor(Config config) {
    this.config = config;
  }

  /**
   * Process a single sequence through the cleaning pipeline
   */
  public Result processOneSequence(String seq) {
    return processOneSequence(seq, null);
  }

  /**
   * Process a single sequence through the cleaning pipeline with an optional ID
   */
  public Result processOneSequence(String seq, String seqId) {
    String raw = seq != null ? seq : "";

    // Stage A: normalize whitespace + uppercase
    // example: "acgtac gta  cgt" -> "ACGTACGTACGT"
    boolean rawHasWs = Pattern.compile("\\s").matcher(raw).find();
    String s0 = raw.replaceAll("\\s+", "");
    String s1 = s0.toUpperCase();

    // Stage B: detect natural language
    // example: "ACGTUNMERGEDACGT" -> naturalLanguageDetected = true
    Pattern naturalLanguagePattern = Pattern.compile(config.naturalLanguageRegex());
    boolean naturalLanguageDetected = naturalLanguagePattern.matcher(s1).find();

    // Stage C: remove gaps
    // example: "ACGT-ACGT..ACGT" -> "ACGTACGTACGT"
    Pattern gapPattern = Pattern.compile(config.gapRegex());
    boolean hasGaps = gapPattern.matcher(s1).find();
    String s2 = s1.replaceAll(config.gapRegex(), "");
    boolean gapsOrWhitespaceRemoved = rawHasWs || hasGaps;

    // Stage D: U -> T (RNA->DNA)
    // example: "ACGTUACGTU" -> "ACGTTACGTT"
    String s3 = s2.replace("U", "T");

    // Stage E: ? -> N
    // example: "ACGT?ACGT" -> "ACGTNACGT"
    String s4 = s3.replace("?", "N");

    // Stage F: trim to anchors (front & back)
    // example: with anchor_chars="ACGTU" and anchor_minrun=8:
    // "THISISMYGBIFSEQUENCEACGTACGTACGTNNNNNENDOFSEQUENCE" -> "ACGTACGTACGT"
    TrimResult tFirst = trimToFirstAnchorOrWipe(s4, config.anchorChars(), config.anchorMinrun());
    String s5 = tFirst.s();
    TrimResult tLast = trimToLastAnchor(s5, config.anchorChars(), config.anchorMinrun());
    String s6 = tLast.s();
    boolean endsTrimmed = tFirst.did() || tLast.did();

    // Stage G: cap N-runs (apply N-run cap to s6 -> s7)
    // example: with nrun_cap_from=6 and nrun_cap_to=5:
    // "ACGTACGTNNNNNNNNNNNNNNACGTACGTNNNNNNNNNACGTACGT" -> "ACGTACGTNNNNNACGTACGTNNNNNACGTACGT"
    String capPattern = "N{" + config.nrunCapFrom() + ",}";
    int nNrunsCapped = countRegex(s6, capPattern);
    String capToStr = "N".repeat(config.nrunCapTo());
    String s7 = s6.replaceAll(capPattern, capToStr);

    // Additional metrics (on s7)
    int sequenceLength = s7.length();
    int nCount = countFixed(s7, 'N');
    Double nFraction = sequenceLength > 0 ? (double) nCount / sequenceLength : null;

    // Compute ambiguous/non-IUPAC counts AFTER capping (on s7)
    int nonAcgtnCount = countRegex(s7, "[^ACGTN]");
    int nonIupacCount = countRegex(s7, "[^" + config.iupacDna() + "]");
    Double nonACGTNFraction = sequenceLength > 0 ? (double) nonAcgtnCount / sequenceLength : null;
    Double nonIupacFraction = sequenceLength > 0 ? (double) nonIupacCount / sequenceLength : null;

    // GC content (A/C/G/T only in denominator)
    int gc = countRegex(s7, "[GC]");
    int acgt = countRegex(s7, "[ACGT]");
    Double gcContent = acgt > 0 ? (double) gc / acgt : null;

    // MD5 of the final cleaned sequence
    String nucleotideSequenceID = md5(s7);

    // Invalid if non-IUPAC characters found or natural language detected
    boolean invalid = (nonIupacFraction != null && nonIupacFraction > 0) || naturalLanguageDetected;

    return new Result(
      seqId,
      raw,
      invalid ? null : s7,
      sequenceLength,
      nonIupacFraction,
      nonACGTNFraction,
      nFraction,
      nNrunsCapped,
      gcContent,
      naturalLanguageDetected,
      endsTrimmed,
      gapsOrWhitespaceRemoved,
      invalid ? null : nucleotideSequenceID,
      invalid
    );
  }

  /**
   * Trims the sequence to the first anchor run of at least minRun consecutive anchor characters.
   * If no valid anchor is found, returns empty string (wipes the sequence).
   */
  private TrimResult trimToFirstAnchorOrWipe(String seq, String anchorChars, int minRun) {
    if (seq == null || seq.isEmpty()) {
      return new TrimResult("", false);
    }

    Pattern anchorPattern = Pattern.compile("[" + anchorChars + "]{" + minRun + ",}");
    Matcher matcher = anchorPattern.matcher(seq);

    if (!matcher.find()) {
      // No valid anchor found - wipe the sequence
      return new TrimResult("", true);
    }

    int startIndex = matcher.start();
    String trimmed = seq.substring(startIndex);

    return new TrimResult(trimmed, startIndex > 0);
  }

  /**
   * Trims the sequence to the last anchor run of at least minRun consecutive anchor characters.
   */
  private TrimResult trimToLastAnchor(String seq, String anchorChars, int minRun) {
    if (seq == null || seq.isEmpty()) {
      return new TrimResult("", false);
    }

    Pattern anchorPattern = Pattern.compile("[" + anchorChars + "]{" + minRun + ",}");
    Matcher matcher = anchorPattern.matcher(seq);

    int lastMatchEnd = -1;
    while (matcher.find()) {
      lastMatchEnd = matcher.end();
    }

    if (lastMatchEnd == -1) {
      return new TrimResult(seq, false);
    }

    String trimmed = seq.substring(0, lastMatchEnd);
    return new TrimResult(trimmed, lastMatchEnd < seq.length());
  }

  /**
   * Counts regex matches in a string
   */
  private int countRegex(String str, String pattern) {
    if (str == null || str.isEmpty()) {
      return 0;
    }
    Matcher matcher = Pattern.compile(pattern).matcher(str);
    int count = 0;
    while (matcher.find()) {
      count++;
    }
    return count;
  }

  /**
   * Counts occurrences of a character in a string
   */
  private int countFixed(String str, char c) {
    if (str == null) {
      return 0;
    }
    int count = 0;
    for (int i = 0; i < str.length(); i++) {
      if (str.charAt(i) == c) {
        count++;
      }
    }
    return count;
  }

  /**
   * Computes MD5 hash of a string
   */
  private String md5(String str) {
    if (str == null || str.isEmpty()) {
      return null;
    }
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] digest = md.digest(str.getBytes());
      StringBuilder sb = new StringBuilder();
      for (byte b : digest) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 algorithm not available", e);
    }
  }
}
