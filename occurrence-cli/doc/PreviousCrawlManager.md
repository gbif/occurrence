The PreviousCrawlsManager is a CLI tool used to emit `delete` messages for occurrence records that are not reference
in the latest version of a dataset. This tool is using Hive over JDBC to read and analyse the current content of the occurrence store.
For regular scenarios, the `OccurrenceHDFS` table is used as source table. It is also possible to use a custom table as long as it includes the columns
`datasetKey`, `gbifId`, `crawlId` but remember `delete` messages will be emitted and the records will be deleted in the HBase table.

### Usage previous-crawls-manager
This cli target is used to manage occurrence records from previous crawls.

Options:
 * `--report-location`: location to save the report (in JSON format)
 * `--display-report`: display the report in the console
 * `--dataset-key`: specify a specific dataset UUID.
 * `--delete`: issue delete occurrence messages if the number of record to delete is below the threshold.
 * `--force-delete`: issue delete occurrence messages even if the number of record to delete is above the threshold. This command can only be used on a specific dataset key.

```bash
java -jar occurrence-cli.jar generate-crawls-report --conf occurrence_crawls_report.yaml
```

### Usage scheduled-previous-crawls-manager
This cli target is used to run the `previous-crawls-manager` on a schedule and normally runs only with the configuration file.

Scheduling related parameters:
```
scheduling:
  frequencyInHour: 24
  startTime: "7:07"
```