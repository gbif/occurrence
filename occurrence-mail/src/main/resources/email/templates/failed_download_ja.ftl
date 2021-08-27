<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Hello ${download.request.creator},
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  We are sorry, but an error has occurred processing your download.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Please see <a href="${portal}occurrence/download/${download.key}" style="color: #509E2F;text-decoration: none;">${portal}occurrence/download/${download.key}</a> for more details, <a href="${portal}health" style="color: #509E2F;text-decoration: none;">${portal}health</a> for the current status of GBIF.org's systems, and try again in a few minutes.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  If the problem persists, contact us using the feedback system on the website, or at <a href="mailto:helpdesk@gbif.org" style="color: #509E2F;text-decoration: none;">helpdesk@gbif.org</a>.  Please include the download key (${download.key}) of the failed download.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>The GBIF Secretariat</em>
</p>

<#include "footer.ftl">
