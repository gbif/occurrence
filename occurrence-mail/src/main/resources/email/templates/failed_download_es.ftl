<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="padding: 0;margin-bottom: 16px;line-height: 1.65;">
  Hola ${download.request.creator},
</h5>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Lo sentimos pero se ha producido un error al procesar su descarga.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Lo lamentamos, pero ha ocurrido un error procesando su archivo de descarga.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Si el problema persiste, contáctenos utilizando el sistema de comentarios del sitio web o escribiendo a <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a>.  Por favor, incluya el identificador (${download.key}) de la descarga fallida.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>Secretariado de GBIF</em>
</p>

<#include "footer.ftl">
