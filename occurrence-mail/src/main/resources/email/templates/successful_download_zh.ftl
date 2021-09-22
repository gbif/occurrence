<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">${download.request.creator}，您好！</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  您的下载地址如下：
  <br>
  <a href="${download.downloadLink}" style="color: #4ba2ce;text-decoration: none;">${download.downloadLink}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">引用</h5>
<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  当使用此数据集时，<strong>请使用以下引用：</strong>
</p>
<p style="background: rgba(190, 198, 206, 0.25);margin: 0 0 20px;padding: 10px;line-height: 1.65;">
  GBIF.org (${downloadCreatedDateDefaultLocale}) GBIF Occurrence Download <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">下载的信息</h5>
<p style="margin: 0;padding: 0;line-height: 1.65;">
  DOI: <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
  （激活前可能需要几个小时）
<br>
  创建日期：${download.created?datetime}
<br>
  记录包括：${download.totalRecords} 来自 ${download.numberDatasets!0} 已发布数据集的记录
<br>
  压缩数据大小： ${size}
<br>
  下载格式： <#if download.request.format == "SIMPLE_CSV"> 简单的tab分隔的值(TSV)<#else>${download.request.format}</#if>
<br>
  使用的筛选条件：
  <pre style="white-space: pre-wrap;margin: 0;padding: 0;">${query}</pre>
</p>


<h5 style="margin: 20px 0;padding: 0;font-size: 16px;line-height: 1.25;">下载文件保留</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  有关此下载的信息可以通过 <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
  和 <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a> 获得。
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <#if download.request.format == "SIMPLE_CSV">简单的tab分隔的值 (TSV)<#else>${download.request.format}</#if>文件将保留6个月（直到${download.eraseAfter?date}）。  您可以从 <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a> 要求我们将文件保留更长时间
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  如果您使用 DOI 来引用下载，我们通常会检测到，并无限期地保留文件。
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  关于该问题的更多信息，请参阅 <a href="${portal}faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #4ba2ce;text-decoration: none;">${portal}faq/?question=for-how-long-will-does-gbif-store-downloads</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">信息 / 常见问题</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  打开下载文件的帮助，请参阅
  <a href="${portal}faq?question=opening-gbif-csv-in-excel" style="color: #4ba2ce;text-decoration: none;">${portal}faq?question=opening-gbif-csv-in-excel</a>
  或 GBIF网站常见问题部分：
  <a href="${portal}faq" style="color: #4ba2ce;text-decoration: none;">${portal}faq</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>GBIF 秘书处</em>
</p>

<#include "footer.ftl">
