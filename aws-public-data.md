# GBIF Public Datasets on Amazon Web Services

This describes the format and gives simple examples for getting started with the GBIF monthly snapshots stored on AWS.

## Data format

Data are stored in Parquet format files in AWS S3 in five regions: af-south-1, ap-southeast-2, eu-central-1, sa-east-1 and us-east-1.  The buckets are as follows:

| Region         | S3 URI                                | Amazon Resource Name (ARN)                   | Browse                                                                                     |
|----------------|---------------------------------------|----------------------------------------------|--------------------------------------------------------------------------------------------|
| af-south-1     | `s3://gbif-open-data-af-south-1/`     | `arn:aws:s3:::gbif-open-data-af-south-1`     | [Browse](https://gbif-open-data-af-south-1.s3.af-south-1.amazonaws.com/index.html)         |
| ap-southeast-2 | `s3://gbif-open-data-ap-southeast-2/` | `arn:aws:s3:::gbif-open-data-ap-southeast-2` | [Browse](https://gbif-open-data-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/index.html) |
| eu-central-1   | `s3://gbif-open-data-eu-central-1/`   | `arn:aws:s3:::gbif-open-data-eu-central-1`   | [Browse](https://gbif-open-data-eu-central-1.s3.eu-central-1.amazonaws.com/index.html)     |
| sa-east-1      | `s3://gbif-open-data-sa-east-1/`      | `arn:aws:s3:::gbif-open-data-sa-east-1`      | [Browse](https://gbif-open-data-sa-east-1.s3.sa-east-1.amazonaws.com/index.html)           |
| us-east-1      | `s3://gbif-open-data-us-east-1/`      | `arn:aws:s3:::gbif-open-data-us-east-1`      | [Browse](https://gbif-open-data-us-east-1.s3.us-east-1.amazonaws.com/index.html)           |

Within that bucket, the periodic occurrence snapshots are stored in `occurrence/YYYY-MM-DD`, where `YYYY-MM-DD` corresponds to the date of the snapshot.

The snapshot includes all CC0, CC-BY and CC-BY-NC licensed data published through GBIF.

Each snapshot contains a `citation.txt` with instructions on how best to cite the data, and the data files themselves in Parquet format: `occurrence.parquet/*`.

Therefore, the data files for the first snapshot are at

`s3://gbif-open-data-REGION/occurrence/2021-04-13/occurrence.parquet/*`

and the citation information is at

`s3://gbif-open-data-REGION/occurrence/2021-04-13/citation.txt`

The Parquet file schema is described here.
Most field names correspond to [terms from the Darwin Core standard](https://dwc.tdwg.org/terms/), and have been interpreted by GBIF's systems to align taxonomy, location, dates etc.
Additional information may be retrived using the [GBIF API](https://www.gbif.org/developer/summary).

|              Field¹              |     Type      | Nullable | Description                   |
|----------------------------------|---------------|----------|-------------------------------|
| gbifid                           | BigInt        | N        | GBIF's identifier for the occurrence |
| datasetkey                       | String (UUID) | N        | GBIF's UUID for the [dataset](https://www.gbif.org/developer/registry#datasets) containing this occurrence |
| publishingorgkey                 | String (UUID) | N        | GBIF's UUID for the [organization](https://www.gbif.org/developer/registry#organizations) publishing this occurrence. |
| occurrencestatus                 | String        | N        | See [dwc:occurrenceStatus](https://dwc.tdwg.org/terms/#occurrenceStatus). Either the value `PRESENT` or `ABSENT`.  **Many users will wish to filter for `PRESENT` data.** |
| basisofrecord                    | String        | N        | See [dwc:basisOfRecord](https://dwc.tdwg.org/terms/#basisOfRecord).  One of `PRESERVED_SPECIMEN`, `FOSSIL_SPECIMEN`, `LIVING_SPECIMEN`, `OBSERVATION`, `HUMAN_OBSERVATION`, `MACHINE_OBSERVATION`, `MATERIAL_SAMPLE`, `MATERIAL_CITATION`, `OCCURRENCE`. |
| kingdom                          | String        | Y        | See [dwc:kingdom](https://dwc.tdwg.org/terms/#kingdom).  This field has been aligned with the [GBIF backbone taxonomy](https://doi.org/10.15468/39omei). |
| phylum                           | String        | Y        | See [dwc:phylum](https://dwc.tdwg.org/terms/#phylum).  This field has been aligned with the GBIF backbone taxonomy. |
| class                            | String        | Y        | See [dwc:class](https://dwc.tdwg.org/terms/#class).  This field has been aligned with the GBIF backbone taxonomy. |
| order                            | String        | Y        | See [dwc:order](https://dwc.tdwg.org/terms/#order).  This field has been aligned with the GBIF backbone taxonomy. |
| family                           | String        | Y        | See [dwc:family](https://dwc.tdwg.org/terms/#family).  This field has been aligned with the GBIF backbone taxonomy. |
| genus                            | String        | Y        | See [dwc:genus](https://dwc.tdwg.org/terms/#genus).  This field has been aligned with the GBIF backbone taxonomy. |
| species                          | String        | Y        | See [dwc:species](https://dwc.tdwg.org/terms/#species).  This field has been aligned with the GBIF backbone taxonomy. |
| infraspecificepithet             | String        | Y        | See [dwc:infraspecificEpithet](https://dwc.tdwg.org/terms/#infraspecificEpithet).  This field has been aligned with the GBIF backbone taxonomy. |
| taxonrank                        | String        | Y        | See [dwc:taxonRank](https://dwc.tdwg.org/terms/#taxonRank).  This field has been aligned with the GBIF backbone taxonomy. |
| scientificname                   | String        | Y        | See [dwc:scientificName](https://dwc.tdwg.org/terms/#scientificName).  This field has been aligned with the GBIF backbone taxonomy. |
| verbatimscientificname           | String        | Y        | The scientific name as provided by the data publisher |
| verbatimscientificnameauthorship | String        | Y        | The scientific name authorship provided by the data publisher. |
| taxonkey                         | Integer       | Y        | The numeric identifier for the [taxon](https://www.gbif.org/developer/species#nameUsages) in GBIF's backbone taxonomy corresponding to `scientificname`. |
| specieskey                       | Integer       | Y        | The numeric identifier for the taxon in GBIF's backbone taxonomy corresponding to `species`. |
| typestatus                       | String array  | N³       | See [dwc:typeStatus](https://dwc.tdwg.org/terms/#typeStatus). |
| countrycode                      | String        | Y        | See [dwc:countryCode](https://dwc.tdwg.org/terms/#countryCode).  GBIF's interpretation has set this to an ISO 3166-2 code. |
| locality                         | String        | Y        | See [dwc:locality](https://dwc.tdwg.org/terms/#locality). |
| stateprovince                    | String        | Y        | See [dwc:stateProvince](https://dwc.tdwg.org/terms/#stateProvince). |
| decimallatitude                  | Double        | Y        | See [dwc:decimalLatitude](https://dwc.tdwg.org/terms/#decimalLatitude).  GBIF's interpretation has normalized this to a WGS84 coordinate. |
| decimallongitude                 | Double        | Y        | See [dwc:decimalLongitude](https://dwc.tdwg.org/terms/#decimalLongitude).  GBIF's interpretation has normalized this to a WGS84 coordinate. |
| coordinateuncertaintyinmeters    | Double        | Y        | See [dwc:coordinateUncertaintyInMeters](https://dwc.tdwg.org/terms/#coordinateUncertaintyInMeters). |
| coordinateprecision              | Double        | Y        | See [dwc:coordinatePrecision](https://dwc.tdwg.org/terms/#coordinatePrecision). |
| elevation                        | Double        | Y        | See [dwc:elevation](https://dwc.tdwg.org/terms/#elevation).  If provided by the data publisher, GBIF's interpretation has normalized this value to metres. |
| elevationaccuracy                | Double        | Y        | See [dwc:elevationAccuracy](https://dwc.tdwg.org/terms/#elevationAccuracy).  If provided by the data publisher, GBIF's interpretation has normalized this value to metres. |
| depth                            | Double        | Y        | See [dwc:depth](https://dwc.tdwg.org/terms/#depth).  If provided by the data publisher, GBIF's interpretation has normalized this value to metres. |
| depthaccuracy                    | Double        | Y        | See [dwc:depthAccuracy](https://dwc.tdwg.org/terms/#depthAccuracy).  If provided by the data publisher, GBIF's interpretation has normalized this value to metres. |
| eventdate                        | Timestamp     | Y        | See [dwc:eventDate](https://dwc.tdwg.org/terms/#eventDate).  GBIF's interpretation has normalized this value to an ISO 8601 date with a local time. |
| year                             | Integer       | Y        | See [dwc:year](https://dwc.tdwg.org/terms/#year). |
| month                            | Integer       | Y        | See [dwc:month](https://dwc.tdwg.org/terms/#month). |
| day                              | Integer       | Y        | See [dwc:day](https://dwc.tdwg.org/terms/#day). |
| individualcount                  | Integer       | Y        | See [dwc:individualCount](https://dwc.tdwg.org/terms/#individualCount). |
| establishmentmeans               | String        | Y        | See [dwc:establishmentMeans](https://dwc.tdwg.org/terms/#establishmentMeans). |
| occurrenceid                     | String        | Y²       | See [dwc:occurrenceID](https://dwc.tdwg.org/terms/#occurrenceID). |
| institutioncode                  | String        | Y²       | See [dwc:institutionCode](https://dwc.tdwg.org/terms/#institutionCode). |
| collectioncode                   | String        | Y²       | See [dwc:collectionCode](https://dwc.tdwg.org/terms/#collectionCode). |
| catalognumber                    | String        | Y²       | See [dwc:catalogNumber](https://dwc.tdwg.org/terms/#catalogNumber). |
| recordnumber                     | String        | Y        | See [dwc:recordNumber](https://dwc.tdwg.org/terms/#recordNumber). |
| recordedby                       | String array  | N³       | See [dwc:recordedBy](https://dwc.tdwg.org/terms/#recordedBy). |
| identifiedby                     | String array  | N³       | See [dwc:identifiedBy](https://dwc.tdwg.org/terms/#identifiedBy). |
| dateidentified                   | Timestamp     | Y        | See [dwc:dateIdentified](https://dwc.tdwg.org/terms/#dateIdentified). An ISO 8601 date. |
| mediatype                        | String array  | N³       | See [dwc:mediaType](https://dwc.tdwg.org/terms/#mediaType).  May contain `StillImage`, `MovingImage` or `Sound` (from [enumeration](http://api.gbif.org/v1/enumeration/basic/MediaType), detailing whether the occurrence has this media available. |
| issue                            | String array  | N³       | A list of [issues](https://gbif.github.io/gbif-api/apidocs/org/gbif/api/vocabulary/OccurrenceIssue.html) encountered by GBIF in processing this record. |
| license                          | String        | N        | See [dwc:license](https://dwc.tdwg.org/terms/#license). Either [`CC0_1_0`](https://creativecommons.org/publicdomain/zero/1.0/), [`CC_BY_4_0`](https://creativecommons.org/licenses/by/4.0) or [`CC_BY_NC_4_0`](https://creativecommons.org/licenses/by-nc/4.0). |
| rightsholder                     | String        | Y        | See [dwc:rightsHolder](https://dwc.tdwg.org/terms/#rightsHolder). |
| lastinterpreted                  | Timestamp     | N        | The ISO 8601 date when the record was last processed by GBIF. Data are reprocessed for several reasons, including changes to the backbone taxonomy, so this date is not necessarily the date the occurrence record last changed. |

¹ Field names are lower case, but in later snapshots this may change to camelCase, for consistency with Darwin Core and the GBIF API.

² Either `occurrenceID`, or `institutionCode` + `collectionCode` + `catalogNumber`, or both, will be present on every record.

³ The array may be empty.

### Change history

Snapshots from 2021-04-13 included only CC0 and CC-BY data.

From 2022-04-01, snapshots include all data (CC0, CC-BY, CC-BY-NC).  Filter using the `license` column if you need to exclude CC-BY-NC data.

From 2022-05-01, the timestamp fields `eventDate`, `dateIdentified` and `lastIntepreted` have a timestamp type, rather than the previous string type.  The fields `identifiedby`, `recordedby` and `typestatus` are changed from a string type to a string array.

## Getting started with Python

You will need to install the libraries, for example using PIP (`pip install boto3 pyarrow`).  You can then run this Python script:

```python
import io
import boto3
import botocore
from botocore import UNSIGNED
from botocore.config import Config
import pyarrow.parquet as pq

BUCKET_NAME = 'gbif-open-data-us-east-1'
# Just read a single (~100MB) chunk of the table
PATH = 'occurrence/2021-04-13/occurrence.parquet/000000'

buffer = io.BytesIO()
s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
s3_object = s3.Object(BUCKET_NAME, PATH)
s3_object.download_fileobj(buffer)

table = pq.read_table(buffer)
df = table.to_pandas()
df.head()

df.groupby(by='kingdom')['gbifid'].count().reset_index(name='count').sort_values(['count'], ascending=False)
```

Output:

```
       gbifid                            datasetkey  ... mediatype                                              issue
0  2305838350  ad43e954-dd79-4986-ae34-9ccdbd8bf568  ...        []  [COUNTRY_DERIVED_FROM_COORDINATES, GEODETIC_DA...
1  2305838837  ad43e954-dd79-4986-ae34-9ccdbd8bf568  ...        []  [COUNTRY_DERIVED_FROM_COORDINATES, GEODETIC_DA...
2  2305839278  ad43e954-dd79-4986-ae34-9ccdbd8bf568  ...        []  [COUNTRY_DERIVED_FROM_COORDINATES, GEODETIC_DA...
3  2305839610  ad43e954-dd79-4986-ae34-9ccdbd8bf568  ...        []  [COUNTRY_DERIVED_FROM_COORDINATES, GEODETIC_DA...
4  2305840564  ad43e954-dd79-4986-ae34-9ccdbd8bf568  ...        []  [COUNTRY_DERIVED_FROM_COORDINATES, GEODETIC_DA...

          kingdom    count
0        Animalia  1017889
8  incertae sedis   168685
5         Plantae    70203
4           Fungi    23234
2        Bacteria    16506
3       Chromista     8389
7         Viruses     3549
1         Archaea     1398
6        Protozoa      658
```

## Getting started with Athena

Athena provides a pay-per-query SQL service on Amazon, particularly well suited for producing summary counts from GBIF data.
The following steps describe how to get started using Athena on the GBIF dataset.

1. Create an S3 bucket in one of the five regions above to store the results of the queries you will execute
2. Open Athena and change to that region
3. Follow the prompt to choose the location where query results should be stored
4. Create a table, by pasting the following command in the query window (change the location to use the snapshot of interest to you, and change `us-east-1` to the region you are using)

```sql
CREATE EXTERNAL TABLE gbif_20220501 (
  `gbifid` bigint,
  `datasetkey` string,
  `occurrenceid` string,
  `kingdom` string,
  `phylum` string,
  `class` string,
  `order` string,
  `family` string,
  `genus` string,
  `species` string,
  `infraspecificepithet` string,
  `taxonrank` string,
  `scientificname` string,
  `verbatimscientificname` string,
  `verbatimscientificnameauthorship` string,
  `countrycode` string,
  `locality` string,
  `stateprovince` string,
  `occurrencestatus` string,
  `individualcount` int,
  `publishingorgkey` string,
  `decimallatitude` double,
  `decimallongitude` double,
  `coordinateuncertaintyinmeters` double,
  `coordinateprecision` double,
  `elevation` double,
  `elevationaccuracy` double,
  `depth` double,
  `depthaccuracy` double,
  `eventdate` timestamp,
  `day` int,
  `month` int,
  `year` int,
  `taxonkey` int,
  `specieskey` int,
  `basisofrecord` string,
  `institutioncode` string,
  `collectioncode` string,
  `catalognumber` string,
  `recordnumber` string,
  `identifiedby` array<string>,
  `dateidentified` timestamp,
  `license` string,
  `rightsholder` string,
  `recordedby` array<string>,
  `typestatus` array<string>,
  `establishmentmeans` string,
  `lastinterpreted` timestamp,
  `mediatype` array<string>,
  `issue` array<string>)
STORED AS parquet
LOCATION
  's3://gbif-open-data-us-east-1/occurrence/2022-05-01/occurrence.parquet/';
```

5. Execute a query

```
SELECT kingdom, count(*) AS c
FROM gbif_20220501
GROUP BY kingdom;
```

Results (these figures are from an older snapshot):

|   | kingdom        | c          |
|---|----------------|------------|
| 1 | Plantae        | 193391585  |
| 2 | incertae sedis | 4223619    |
| 3 | Archaea        | 226905     |
| 4 | Fungi          | 12210273   |
| 5 | Viruses        | 42019      |
| 6 | Bacteria       | 13313089   |
| 7 | Animalia       | 1064194305 |
| 8 | Protozoa       | 793176     |
| 9 | Chromista      | 9440667    |

5. Your results should show in the browser, and will also be stored as CSV data in the S3 bucket you created
6. The amount of data scanned will be shown, which is used to calculate the billing (44.89MB, which is a fraction of a cent for this query, see [Amazon Athena pricing](https://aws.amazon.com/athena/pricing/))

## Downloading/mirroring the data

A monthly snapshot is roughly 180GB in size.

The AWS buckets are public, and can be accessed anonymously using the [S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html), [AWS CLI tool](https://aws.amazon.com/cli/), or tools like [rclone](https://rclone.org).

```
# AWS CLI
aws --no-sign-request --region eu-central-1 s3 ls s3://gbif-open-data-eu-central-1/occurrence/

# rclone configuration
[anons3]
type = s3
provider = AWS
env_auth = false
access_key_id =
secret_access_key =
region = eu-central-1
endpoint =
location_constraint =
acl = private
server_side_encryption =
storage_class =

# rclone commands
rclone ls anons3:gbif-open-data-eu-central-1
rclone sync -v anons3:gbif-open-data-eu-central-1/occurrence/2021-04-13/ ./gbif_2021-04-13/
```

The SNS endpoints can be used to follow changes.
