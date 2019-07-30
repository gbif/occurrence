<#-- @ftlvariable name="" type="org.gbif.occurrence.download.service.EmailModel" -->
Hello ${download.request.creator},

Your download is available at the following address:
    ${download.downloadLink}

When using this dataset please use the following citation:
    GBIF.org (${niceDate(download.created)}) GBIF Occurrence Download ${download.doi.getUrl()}

Download Information:
    DOI: ${download.doi.getUrl()} (may take some hours before being active)
    Creation Date: ${download.created?datetime?string.full}
    Records included: ${download.totalRecords} records from ${download.numberDatasets!0} published datasets
    Data size: ${size}
    Download format: ${download.request.format}
    Filter used:
${query!"        All occurrence records"}

Download file retention:
    Information about this download will always be available at ${download.doi.getUrl()} and ${portal}occurrence/download/${download.key}

    The ${download.request.format} ZIP file will be kept for six months (until ${niceDate(download.eraseAfter)}).  You can ask
    us to keep the file for longer from ${portal}occurrence/download/${download.key}

    If you cite this download using the DOI, we will usually detect this and keep the file indefinitely.

    For more information on this, see ${portal}faq/?question=for-how-long-will-does-gbif-store-downloads

For help with opening downloaded files, see
    ${portal}faq?question=opening-gbif-csv-in-excel
or the FAQ section of the GBIF website:
    ${portal}faq
