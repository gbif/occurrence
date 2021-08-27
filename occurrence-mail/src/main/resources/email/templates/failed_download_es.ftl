<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Hola ${download.request.creator},
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Lo lamentamos, pero ha ocurrido un error procesando su archivo de descarga.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Por más detalles, por favor consultar <a href="${portal}es/occurrence/download/${download.key}" style="color: #509E2F;text-decoration: none;">${portal}es/occurrence/download/${download.key}</a> <br>
  Consulte el estado de los servicios de GBIF en <a href="${portal}es/health" style="color: #509E2F;text-decoration: none;">${portal}health</a>, e intente de nuevo en unos minutos.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Si el problema persiste, contáctenos utilizando la funcionalidad de retroalimentación del sitio web, ó escribiendo a <a href="mailto:helpdesk@gbif.org" style="color: #509E2F;text-decoration: none;">helpdesk@gbif.org</a>.<br>
  Por favor incluya la identificación (${download.key}) del archivo de descarga fallido.  Please include the download key (${download.key}) of the failed download.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>Secretariado de GBIF</em>
</p>

<#include "footer.ftl">
