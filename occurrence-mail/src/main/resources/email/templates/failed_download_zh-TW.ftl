<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="padding: 0;margin-bottom: 16px;line-height: 1.65;">
  Hello ${download.request.creator},
</h5>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  很抱歉，您的下載在過程中發生錯誤。
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Please see <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a> for more details, <a href="${portal}system-health" style="color: #4ba2ce;text-decoration: none;">${portal}system-health</a> for the current status of GBIF.org's systems, and try again in a few minutes.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  若問題持續存在，請透過網站上的問題回饋系統聯絡我們，或寫信到<a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a>。  Please include the download key (${download.key}) of the failed download.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>GBIF 秘書處</em>
</p>

<#include "footer.ftl">
