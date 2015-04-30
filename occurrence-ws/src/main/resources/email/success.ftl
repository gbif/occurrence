<#-- @ftlvariable name="" type="org.gbif.occurrence.download.service.EmailModel" -->
Hello ${download.request.creator},

your download is available on the following address:
    ${download.downloadLink}

When using this dataset please use the following citation:
    GBIF.org (${niceDate(download.created)}) GBIF Occurrence Download ${download.doi.getUrl()}

Download Information:
    DOI: ${download.doi.getUrl()} (may take some hours before being active)
    Creation Date: ${download.created?datetime?string.full}
    Records included: ${download.totalRecords} records from ${download.numberDatasets!0} published datasets
    Data size: ${size}
    Download format: ${download.request.format}
    Filter used: ${query!"All occurrence records"}

This download can always be viewed on
    ${portal}occurrence/download/${download.key}

For information about download formats please visit
    ${portal}faq/datause
