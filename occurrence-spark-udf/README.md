# GBIF Occurrence Spark UDF

A separate module to hold the Spark User Defined Functions (UDFs) used for downloads.

## Building

```bash
mvn clean package -DskipTests
```

This produces:
- `target/occurrence-spark-udf-<version>.jar` - The main UDF library

## Available UDFs

| UDF Name | Class | Description |
|----------|-------|-------------|
| `processSequence` | `SequenceProcessorUdf` | DNA/RNA sequence cleaning and validation |
| `cleanDelimiters` | `CleanDelimiterCharsUdf` | Clean delimiter characters from strings |
| `cleanDelimitersArray` | `CleanDelimiterArraysUdf` | Clean delimiter characters from string arrays |
| `secondsToISO8601` | `SecondsToISO8601Udf` | Convert epoch seconds to ISO8601 |
| `secondsToLocalISO8601` | `SecondsToLocalISO8601Udf` | Convert epoch seconds to local ISO8601 |
| `millisecondsToISO8601` | `MillisecondsToISO8601Udf` | Convert epoch milliseconds to ISO8601 |
| `contains` | `ContainsUdf` | Check if a geometry contains a point |
| `geoDistance` | `GeoDistanceUdf` | Check if point is within distance of another |
| `stringArrayContains` | `StringArrayContainsGenericUdf` | Check if array contains a value |
| `stringArrayLike` | `StringArrayLikeGenericUdf` | Check if array element matches pattern |

## SequenceProcessorUdf

The `SequenceProcessorUdf` processes DNA/RNA sequences through a cleaning pipeline and returns quality metrics.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `sequence` | String | Yes | - | The raw DNA/RNA sequence |
| `seqId` | String | No | null | Optional sequence identifier |
| `anchorChars` | String | No | "ACGTU" | Characters considered as valid anchors |
| `anchorMinrun` | Integer | No | 8 | Minimum consecutive anchor chars to find valid sequence |
| `anchorStrict` | String | No | "ACGTU" | Strict anchor characters |
| `gapRegex` | String | No | "[-\\.]" | Regex pattern for gap characters to remove |
| `naturalLanguageRegex` | String | No | "UNMERGED" | Pattern to detect natural language contamination |
| `iupacRna` | String | No | "ACGTURYSWKMBDHVN" | Valid IUPAC RNA characters |
| `iupacDna` | String | No | "ACGTRYSWKMBDHVN" | Valid IUPAC DNA characters |
| `nrunCapFrom` | Integer | No | 6 | Cap N-runs of this length or more |
| `nrunCapTo` | Integer | No | 5 | Cap N-runs to this length |

### Return Schema

Returns a struct with the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `seqId` | String | The sequence identifier |
| `rawSequence` | String | Original input sequence |
| `sequence` | String | Cleaned sequence (null if invalid) |
| `sequenceLength` | Integer | Length of cleaned sequence |
| `nonIupacFraction` | Double | Fraction of non-IUPAC characters |
| `nonACGTNFraction` | Double | Fraction of non-ACGTN characters |
| `nFraction` | Double | Fraction of N characters |
| `nNrunsCapped` | Integer | Number of N-runs that were capped |
| `gcContent` | Double | GC content ratio |
| `naturalLanguageDetected` | Boolean | Whether natural language was detected |
| `endsTrimmed` | Boolean | Whether sequence ends were trimmed |
| `gapsOrWhitespaceRemoved` | Boolean | Whether gaps/whitespace were removed |
| `nucleotideSequenceID` | String | MD5 hash of cleaned sequence |
| `invalid` | Boolean | Whether sequence is invalid |

## Testing in Spark on Kubernetes

### Prerequisites

1. A running Kubernetes cluster with Spark operator or spark-submit capability
2. Access to a container registry (e.g., Docker Hub, GCR, ECR)
3. kubectl configured to access your cluster

### Step 1: Build the JAR

```bash
mvn clean package -DskipTests
```

### Step 2: Upload JAR to accessible storage

The JAR needs to be accessible from your Spark pods. Options include:

**Option A: Include in custom Spark image**
```dockerfile
FROM apache/spark:3.5.0
COPY target/occurrence-spark-udf-*.jar /opt/spark/jars/
```

**Option B: Upload to cloud storage (S3, GCS, HDFS)**
```bash
# For S3
aws s3 cp target/occurrence-spark-udf-*.jar s3://your-bucket/jars/

# For GCS
gsutil cp target/occurrence-spark-udf-*.jar gs://your-bucket/jars/

# For HDFS
hdfs dfs -put target/occurrence-spark-udf-*.jar /user/spark/jars/
```

**Option C: Use Maven repository (recommended for released versions)**

If the JAR is published to a Maven repository (e.g., Maven Central, GBIF repository), Spark can download it automatically using `--packages`:

```bash
# Using Maven coordinates (groupId:artifactId:version)
spark-submit \
  --packages org.gbif.occurrence:occurrence-spark-udf:1.1.21-SNAPSHOT \
  --repositories https://repository.gbif.org/repository/releases/ \
  ...
```

Or configure in spark-defaults.conf:
```properties
spark.jars.packages=org.gbif.occurrence:occurrence-spark-udf:1.1.21-SNAPSHOT
spark.jars.repositories=https://repository.gbif.org/repository/releases/
```

### Step 3: Submit Spark job with spark-submit

**Using JAR from HDFS:**
```bash
spark-submit \
  --master k8s://https://<k8s-api-server>:6443 \
  --deploy-mode cluster \
  --name test-udf \
  --conf spark.kubernetes.container.image=apache/spark:3.5.0 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --jars hdfs://<namenode>:8020/user/spark/jars/occurrence-spark-udf-1.1.21-SNAPSHOT.jar \
  --class com.example.YourMainClass \
  hdfs://<namenode>:8020/user/spark/apps/your-app.jar
```

**Using JAR from Maven repository:**
```bash
spark-submit \
  --master k8s://https://<k8s-api-server>:6443 \
  --deploy-mode cluster \
  --name test-udf \
  --conf spark.kubernetes.container.image=apache/spark:3.5.0 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --packages org.gbif.occurrence:occurrence-spark-udf:1.1.21-SNAPSHOT \
  --repositories https://repository.gbif.org/repository/releases/ \
  --class com.example.YourMainClass \
  local:///opt/spark/examples/your-app.jar
```

**Using JAR from S3:**
```bash
spark-submit \
  --master k8s://https://<k8s-api-server>:6443 \
  --deploy-mode cluster \
  --name test-udf \
  --conf spark.kubernetes.container.image=apache/spark:3.5.0 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET_KEY \
  --jars s3a://your-bucket/jars/occurrence-spark-udf-1.1.21-SNAPSHOT.jar \
  --class com.example.YourMainClass \
  s3a://your-bucket/apps/your-app.jar
```

## Using with Spark SQL

### Starting spark-sql CLI with the UDF JAR

**From HDFS:**
```bash
spark-sql \
  --jars hdfs://<namenode>:8020/user/spark/jars/occurrence-spark-udf-1.1.21-SNAPSHOT.jar
```

**From Maven repository:**
```bash
spark-sql \
  --packages org.gbif.occurrence:occurrence-spark-udf:1.1.21-SNAPSHOT \
  --repositories https://repository.gbif.org/repository/releases/
```

**From local file:**
```bash
spark-sql \
  --jars /path/to/occurrence-spark-udf-1.1.21-SNAPSHOT.jar
```

**From S3:**
```bash
spark-sql \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET_KEY \
  --jars s3a://your-bucket/jars/occurrence-spark-udf-1.1.21-SNAPSHOT.jar
```

### Registering UDFs in Spark SQL

Once spark-sql is running, you need to register the UDFs. You can do this using the `ADD JAR` command and then creating temporary functions:

```sql
-- If JAR not already in classpath, add it
ADD JAR hdfs://<namenode>:8020/user/spark/jars/occurrence-spark-udf-1.1.21-SNAPSHOT.jar;

-- Register the SequenceProcessorUdf
CREATE OR REPLACE TEMPORARY FUNCTION processSequence
AS 'org.gbif.occurrence.spark.udf.SequenceProcessorUdf';

-- Register other UDFs as needed
CREATE OR REPLACE TEMPORARY FUNCTION cleanDelimiters
AS 'org.gbif.occurrence.spark.udf.CleanDelimiterCharsUdf';

CREATE OR REPLACE TEMPORARY FUNCTION geoDistance
AS 'org.gbif.occurrence.spark.udf.GeoDistanceUdf';
```

### Using processSequence UDF in SQL Queries

**Basic usage with default config (pass nulls for optional params):**
```sql
-- Create a test table
CREATE TEMPORARY VIEW sequences AS
SELECT 'ACGTACGTACGTACGT' as sequence, 'seq1' as id
UNION ALL
SELECT 'acgt-acgt-acgt-acgt', 'seq2'
UNION ALL
SELECT 'ACGUACGUACGUACGU', 'seq3'
UNION ALL
SELECT 'ACGTNNNNNNNNNNACGTACGTACGT', 'seq4';

-- Process sequences with default config
SELECT
    id,
    processSequence(sequence, id, null, null, null, null, null, null, null, null, null).sequence as cleaned_sequence,
    processSequence(sequence, id, null, null, null, null, null, null, null, null, null).sequenceLength as length,
    processSequence(sequence, id, null, null, null, null, null, null, null, null, null).gcContent as gc_content,
    processSequence(sequence, id, null, null, null, null, null, null, null, null, null).invalid as is_invalid
FROM sequences;
```

**Extract all fields from the result struct:**
```sql
SELECT
    id,
    result.*
FROM (
    SELECT
        id,
        processSequence(sequence, id, null, null, null, null, null, null, null, null, null) as result
    FROM sequences
) t;
```

**With custom configuration:**
```sql
-- Custom config: anchorMinrun=4, nrunCapFrom=4, nrunCapTo=2
SELECT
    id,
    processSequence(
        sequence,           -- sequence
        id,                 -- seqId
        'ACGTU',           -- anchorChars
        4,                  -- anchorMinrun (lower threshold)
        'ACGTU',           -- anchorStrict
        '[-\\.]',          -- gapRegex
        'UNMERGED',        -- naturalLanguageRegex
        'ACGTURYSWKMBDHVN', -- iupacRna
        'ACGTRYSWKMBDHVN',  -- iupacDna
        4,                  -- nrunCapFrom
        2                   -- nrunCapTo
    ) as result
FROM sequences;
```

**Filter invalid sequences:**
```sql
SELECT
    id,
    result.sequence,
    result.sequenceLength,
    result.gcContent
FROM (
    SELECT
        id,
        processSequence(sequence, id, null, null, null, null, null, null, null, null, null) as result
    FROM sequences
) t
WHERE result.invalid = false;
```

**Aggregate statistics:**
```sql
SELECT
    COUNT(*) as total_sequences,
    SUM(CASE WHEN result.invalid THEN 1 ELSE 0 END) as invalid_count,
    AVG(result.sequenceLength) as avg_length,
    AVG(result.gcContent) as avg_gc_content,
    SUM(result.nNrunsCapped) as total_nruns_capped
FROM (
    SELECT
        processSequence(sequence, id, null, null, null, null, null, null, null, null, null) as result
    FROM sequences
) t;
```

### Using with Hive Tables

```sql
-- Process sequences from a Hive table
SELECT
    occurrence_id,
    result.sequence as cleaned_sequence,
    result.sequenceLength,
    result.gcContent,
    result.nucleotideSequenceID,
    result.invalid
FROM (
    SELECT
        occurrence_id,
        processSequence(
            dna_sequence,
            occurrence_id,
            null, null, null, null, null, null, null, null, null
        ) as result
    FROM occurrence_sequences
    WHERE dna_sequence IS NOT NULL
) t;

-- Create a new table with processed sequences
CREATE TABLE processed_sequences AS
SELECT
    occurrence_id,
    result.sequence as cleaned_sequence,
    result.sequenceLength,
    result.nonIupacFraction,
    result.nonACGTNFraction,
    result.nFraction,
    result.nNrunsCapped,
    result.gcContent,
    result.naturalLanguageDetected,
    result.endsTrimmed,
    result.gapsOrWhitespaceRemoved,
    result.nucleotideSequenceID,
    result.invalid
FROM (
    SELECT
        occurrence_id,
        processSequence(dna_sequence, occurrence_id, null, null, null, null, null, null, null, null, null) as result
    FROM occurrence_sequences
) t;
```

### Spark SQL on Kubernetes with spark-sql CLI

```bash
# Start spark-sql in client mode connecting to K8s
spark-sql \
  --master k8s://https://<k8s-api-server>:6443 \
  --deploy-mode client \
  --conf spark.kubernetes.container.image=apache/spark:3.5.0 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --packages org.gbif.occurrence:occurrence-spark-udf:1.1.21-SNAPSHOT \
  --repositories https://repository.gbif.org/repository/releases/
```

### Permanent UDF Registration (Hive Metastore)

To make UDFs available permanently across sessions:

```sql
-- Register as permanent function (requires Hive metastore)
CREATE FUNCTION processSequence
AS 'org.gbif.occurrence.spark.udf.SequenceProcessorUdf'
USING JAR 'hdfs://<namenode>:8020/user/spark/jars/occurrence-spark-udf-1.1.21-SNAPSHOT.jar';

-- Now available in all sessions
SHOW FUNCTIONS LIKE 'processSequence';
```

### Step 4: Interactive Testing with spark-shell

Create a test pod with spark-shell:

```yaml
# spark-shell-pod.yaml
apiVersion: v1
kind: Pod
metadata:
  name: spark-shell-test
spec:
  containers:
  - name: spark
    image: apache/spark:3.5.0
    command: ["/bin/bash", "-c", "sleep infinity"]
    volumeMounts:
    - name: udf-jar
      mountPath: /opt/spark/user-jars
  volumes:
  - name: udf-jar
    configMap:
      name: udf-jar-config
  restartPolicy: Never
```

Then exec into the pod:

```bash
kubectl apply -f spark-shell-pod.yaml
kubectl exec -it spark-shell-test -- /bin/bash

# Inside the pod, start spark-shell with the UDF jar
/opt/spark/bin/spark-shell --jars /opt/spark/user-jars/occurrence-spark-udf-*.jar
```

### Step 5: Register and Test UDFs

In spark-shell or your Spark application:

```scala
// Scala
import org.gbif.occurrence.spark.udf.UDFS
import org.gbif.occurrence.spark.udf.SequenceProcessorUdf

// Register all UDFs
UDFS.registerUdfs(spark)

// Or register just the SequenceProcessorUdf manually
spark.udf.register("processSequence", new SequenceProcessorUdf(), SequenceProcessorUdf.resultSchema())

// Test with sample data
val testData = Seq(
  ("ACGTACGTACGTACGT", "seq1"),
  ("acgt-acgt-acgt-acgt", "seq2"),
  ("ACGUACGUACGUACGU", "seq3"),  // RNA
  ("ACGTNNNNNNNNNNACGT", "seq4") // N-run
).toDF("sequence", "id")

// Process sequences with default config
val result = testData.selectExpr(
  "id",
  "processSequence(sequence, id, null, null, null, null, null, null, null, null, null) as processed"
)

result.select(
  col("id"),
  col("processed.sequence"),
  col("processed.sequenceLength"),
  col("processed.gcContent"),
  col("processed.invalid")
).show(false)

// Process with custom config (e.g., different N-run capping)
val customResult = testData.selectExpr(
  "id",
  "processSequence(sequence, id, 'ACGTU', 4, 'ACGTU', '[-\\\\.]', 'UNMERGED', 'ACGTURYSWKMBDHVN', 'ACGTRYSWKMBDHVN', 4, 2) as processed"
)

customResult.show(false)
```

```python
# PySpark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("UDF Test") \
    .config("spark.jars", "/path/to/occurrence-spark-udf-*.jar") \
    .getOrCreate()

# Register UDF from Java class
spark._jvm.org.gbif.occurrence.spark.udf.UDFS.registerUdfs(spark._jsparkSession)

# Test the UDF
test_data = [
    ("ACGTACGTACGTACGT", "seq1"),
    ("acgt-acgt-acgt-acgt", "seq2"),
]
df = spark.createDataFrame(test_data, ["sequence", "id"])

# Use SQL
df.createOrReplaceTempView("sequences")
result = spark.sql("""
    SELECT
        id,
        processSequence(sequence, id, null, null, null, null, null, null, null, null, null).sequence as cleaned_sequence,
        processSequence(sequence, id, null, null, null, null, null, null, null, null, null).sequenceLength as length,
        processSequence(sequence, id, null, null, null, null, null, null, null, null, null).invalid as is_invalid
    FROM sequences
""")
result.show()
```

### Step 6: Using SparkApplication CRD (Spark Operator)

If using the Spark Operator, create a SparkApplication:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: test-sequence-udf
  namespace: spark
spec:
  type: Scala
  mode: cluster
  image: your-registry/spark-with-udfs:latest
  mainClass: com.example.TestSequenceUdf
  mainApplicationFile: local:///opt/spark/examples/your-app.jar
  sparkVersion: "3.5.0"
  deps:
    jars:
      - local:///opt/spark/jars/occurrence-spark-udf-1.1.21-SNAPSHOT.jar
  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark
  executor:
    cores: 1
    instances: 2
    memory: "1g"
```

### Troubleshooting

**JAR not found:**
- Ensure the JAR path is correct and accessible from both driver and executors
- Check that the JAR is included in the container image or mounted correctly

**Class not found:**
- Verify the JAR contains all required dependencies
- Check `spark.driver.extraClassPath` and `spark.executor.extraClassPath` configs

**UDF registration fails:**
- Ensure Spark version compatibility (built for Scala 2.12/2.13)
- Check for conflicting dependency versions

**View logs:**
```bash
# Driver logs
kubectl logs <driver-pod-name>

# Executor logs
kubectl logs <executor-pod-name>
```

## Running Unit Tests

```bash
mvn test
```

To run a specific test class:
```bash
mvn test -Dtest=SequenceProcessorUdfTest
```

