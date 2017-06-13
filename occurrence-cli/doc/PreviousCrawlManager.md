The PreviousCrawlsManager is a CLI tool used to emit `delete` messages for occurrence records that are not reference
in the latest version of a dataset. This tool uses Hive over JDBC to read and analyse the current content of the occurrence store.
Typical use would be to use the `occurrence_hdfs` table as the source. It is also possible to use a custom table as long as it includes the columns
`datasetKey`, `gbifId`, `crawlId` but remember `delete` messages will be emitted and the records will be deleted in the HBase table.

Warnings:
 * There is no checks performed on the length of the message queue.
 * Remember the `occurrence_hdfs` table is rebuilt daily. Changes will only be visible to this tool after a new `occurrence_hdfs` has been rebuilt.

See example configuration file [here](https://github.com/gbif/occurrence/tree/master/occurrence-cli/example-confs/previous-crawls-manager.yaml).

### Auto-deletion mechanism

Quick explanations of what [PreviousCrawlsManager](https://github.com/gbif/occurrence/blob/master/occurrence-cli/src/main/java/org/gbif/occurrence/cli/crawl/PreviousCrawlsManager.java)
is doing.

 * Get `datasetKey`, `crawlId` and counts of occurrence records grouped by `datasetKey` and `crawlId` (only for dataset with more than 1 crawl)
 * For each entries, decide if we should run auto-deletion:
   * Check that the sum of all previous crawls is less than 30% (configurable) of the current total of occurrence records.
   * Check if last crawl (highest `crawlId`) count matches the number of `fragmentEmitted` in the registry. We want to check if what we identified
  as the last crawl was completed before we generated our source table (`occurrence_hdfs`).

If a dataset is identified suitable for auto-deletion of its previous crawl(s) it is handled by [PreviousCrawlsOccurrenceDeleter](https://github.com/gbif/occurrence/blob/master/occurrence-cli/src/main/java/org/gbif/occurrence/cli/crawl/PreviousCrawlsOccurrenceDeleter.java).
 * Get all `gbifId` for the `datasetKey` where the `crawlId` is less than the last crawl (in case a new crawl started in the meantime)
 * For each `gbifId`:
   * emit a delete messages (in batch)
   * add a comment in the registry

### Usage previous-crawls-manager
This cli target is used to manage occurrence records from previous crawls.

Options:
 * `--report-output-filepath`: file path where to save the report (in JSON format). Warning the content will be overwritten if the file
 already exists. This option can be use with `--display-report`.
 * `--display-report`: display the report in the console. This option can be use with `--report-output-filepath`.
 * `--dataset-key`: specify a specific dataset UUID.
 * `--delete`: issue delete occurrence messages if the number of record to delete is below the threshold.
 * `--force-delete`: issue delete occurrence messages even if the number of record to delete is above the threshold. This command can only be used on a specific dataset key.

Example of displaying a report for a specific dataset:
```bash
java -jar occurrence-cli.jar previous-crawls-manager --dataset-key 6ce9819a-d82b-41e1-9059-0dd201f15993 --display-report --conf previous-crawls-manager.yaml
```

### Usage scheduled-previous-crawls-manager
This cli target is used to run the `previous-crawls-manager` on a schedule and normally runs only with the configuration file.

Scheduling related parameters:
```
scheduling:
  frequencyInHour: 24
  startTime: "07:07"
```

Example of generating a complete report at regular interval:
```bash
java -jar occurrence-cli.jar scheduled-previous-crawls-manager --report-location /tmp/overcrawled_report.json --conf previous-crawls-manager.yaml
```
