<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Hello ${download.request.creator},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Your download is available at the following address:
  <br>
  <a href="${download.downloadLink}" style="color: #509E2F;text-decoration: none;">${download.downloadLink}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Citation</h5>
<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  When using this dataset <strong>please use the following citation:</strong>
</p>
<p style="background: rgba(190, 198, 206, 0.25);margin: 0 0 20px;padding: 10px;line-height: 1.65;">
  GBIF.org (${downloadCreatedDateDefaultLocale}) GBIF Occurrence Download <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Download Information</h5>
<p style="margin: 0;padding: 0;line-height: 1.65;">
  DOI: <a href="${download.doi.getUrl()}" style="color: #509E2F;text-decoration: none;">${download.doi.getUrl()}</a>
  (may take some hours before being active)
<br>
  Creation Date: ${download.created?datetime}
<br>
  Records included: ${download.totalRecords} records from ${download.numberDatasets!0} published datasets
<br>
  Compressed data size: ${size}
<br>
  Download format: <#if download.request.format == "SIMPLE_CSV">simple tab-separated values (TSV)<#else>${download.request.format}</#if>
<br>
  Filter used:
  <pre style="white-space: pre-wrap;margin: 0;padding: 0;">${query}</pre>
</p>


<h5 style="margin: 20px 0;padding: 0;font-size: 16px;line-height: 1.25;">Download file retention</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Information about this download will always be available at <a href="${download.doi.getUrl()}" style="color: #509E2F;text-decoration: none;">${download.doi.getUrl()}</a>
  and <a href="${portal}occurrence/download/${download.key}" style="color: #509E2F;text-decoration: none;">${portal}occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  The <#if download.request.format == "SIMPLE_CSV">simple tab-separated values (TSV)<#else>${download.request.format}</#if> file will be kept for six months (until ${download.eraseAfter?date}).  You can ask
  us to keep the file for longer from <a href="${portal}occurrence/download/${download.key}" style="color: #509E2F;text-decoration: none;">${portal}occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  If you cite this download using the DOI, we will usually detect this and keep the file indefinitely.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  For help with opening downloaded files, see
  <a href="${portal}faq?question=opening-gbif-csv-in-excel" style="color: #509E2F;text-decoration: none;">${portal}faq?question=opening-gbif-csv-in-excel</a>
  or the FAQ section of the GBIF website:
  <a href="${portal}faq" style="color: #509E2F;text-decoration: none;">${portal}faq</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Information / FAQ</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  For more information on this, see <a href="${portal}faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #509E2F;text-decoration: none;">${portal}faq/?question=for-how-long-will-does-gbif-store-downloads</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>Le Secrétariat du GBIF</em>
</p>

<#include "footer.ftl">
