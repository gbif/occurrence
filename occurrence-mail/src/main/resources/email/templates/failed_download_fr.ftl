<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="padding: 0;margin-bottom: 16px;line-height: 1.65;">
  Bonjour ${download.request.creator},
</h5>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Nous sommes désolés, mais une erreur s'est produite lors du traitement de votre téléchargement.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Veuillez consulter <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a> pour plus de détails, <a href="${portal}health" style="color: #4ba2ce;text-decoration: none;">${portal}santé</a> sur l'état actuel des systèmes du GBIF.org, et réessayez dans quelques minutes.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Si le problème persiste, contactez-nous en utilisant le système de commentaires sur le site web, ou à <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a>.  Veuillez inclure la clé de téléchargement (${download.key}) du téléchargement échoué.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>Le Secrétariat du GBIF</em>
</p>

<#include "footer.ftl">
