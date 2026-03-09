# Nucleotide Sequence Processor

Spark UDF for cleaning and validating DNA/RNA sequences.

## Usage

```sql
-- Register the UDF
CREATE FUNCTION processSequence AS 'org.gbif.occurrence.spark.udf.ProcessSequenceUdf';

-- Create nucleotide sequence table
CREATE TABLE prod.occurrence_nucleotide_sequence AS
SELECT gbifid, datasetkey, inline(array(processSequence(dnasequence)))
FROM occurrence_ext_gbif_dnaderiveddata
WHERE dnasequence IS NOT NULL;
```

## Processing Pipeline

1. **Whitespace/gap removal** - Removes gaps (`-`, `.`) and whitespace
2. **Natural language detection** - Flags sequences containing words like "UNMERGED"
3. **End trimming** - Trims non-anchor characters from sequence ends
4. **N-run capping** - Caps consecutive N runs (≥6) to 5
5. **Quality metrics** - Calculates GC content, IUPAC compliance, etc.
6. **MD5 hash** - Generates unique identifier for cleaned sequence

## Output Columns

| Column | Type | Description |
|--------|------|-------------|
| `sequence` | String | Cleaned sequence (null if invalid) |
| `sequenceLength` | Integer | Length of cleaned sequence |
| `gcContent` | Double | GC content ratio |
| `nonIupacFraction` | Double | Fraction of non-IUPAC characters |
| `nonACGTNFraction` | Double | Fraction of non-ACGTN characters |
| `nFraction` | Double | Fraction of N characters |
| `nNrunsCapped` | Integer | Number of N-runs capped |
| `naturalLanguageDetected` | Boolean | Natural language detected |
| `endsTrimmed` | Boolean | Ends were trimmed |
| `gapsOrWhitespaceRemoved` | Boolean | Gaps/whitespace removed |
| `nucleotideSequenceID` | String | MD5 hash of cleaned sequence |
| `invalid` | Boolean | Sequence is invalid |

