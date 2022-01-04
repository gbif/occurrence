<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="padding: 0;margin-bottom: 16px;line-height: 1.65;">
  Hello ${download.request.creator},
</h5>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Desculpe, mas ocorreu um erro ao processar o seu download.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Please see <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a> for more details, <a href="${portal}health" style="color: #4ba2ce;text-decoration: none;">${portal}health</a> for the current status of GBIF.org's systems, and try again in a few minutes.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Se o problema persistir, entre em contato conosco usando o sistema de feedback no site, ou em <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a>.  Please include the download key (${download.key}) of the failed download.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>Secretaria do GBIF</em>
</p>

<#include "footer.ftl">
