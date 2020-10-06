<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Hola ${download.request.creator},
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Lo lamentamos, pero ha ocurrido un error procesando su archivo de descarga.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Por favor ver <a href="${portal}occurrence/download/${download.key}" style="color: #509E2F;text-decoration: none;">${portal}occurrence/download/${download.key}</a> por más detalles, <a href="${portal}health" style="color: #509E2F;text-decoration: none;">${portal}health</a> para consultar el estado de los sistemas de GBIF.org, e intente de nuevo en unos minutos.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Si el problema persiste, contáctenos utilizando la funcionalidad de retroalimentación del sitio web, ó escribiendo a <a href="mailto:helpdesk@gbif.org" style="color: #509E2F;text-decoration: none;">helpdesk@gbif.org</a>. Por favor incluya la identificación (${download.key}) del archivo de descarga fallido.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>El Secretariado de GBIF</em>
</p>

<#include "footer.ftl">
