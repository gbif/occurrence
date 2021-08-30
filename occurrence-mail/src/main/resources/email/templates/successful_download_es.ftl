<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Hola ${download.request.creator},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Tu descarga se encuentra disponible en el siguiente enlace:
  <br>
  <a href="${download.downloadLink}" style="color: #4ba2ce;text-decoration: none;">${download.downloadLink}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Cómo citar</h5>
<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Cuando haga uso de este dataset, <strong>por favor, use la siguiente cita:</strong>
</p>
<p style="background: rgba(190, 198, 206, 0.25);margin: 0 0 20px;padding: 10px;line-height: 1.65;">
  GBIF.org (${downloadCreatedDateDefaultLocale}) GBIF Occurrence Download <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Información sobre la descarga</h5>
<p style="margin: 0;padding: 0;line-height: 1.65;">
  DOI: <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
  (pueden pasar varias horas hasta que esté activo)
<br>
  Fecha de creación: ${download.created?datetime}
<br>
  Registros incluidos: ${download.totalRecords} registros de ${download.numberDatasets!0} datasets publicados
<br>
  Tamaño de los datos comprimidos: ${size}
<br>
  Formato de la descarga: <#if download.request.format == "SIMPLE_CSV">simple tab-separated values (TSV)<#else>${download.request.format}</#if>
<br>
  Filtro usado:
  <pre style="white-space: pre-wrap;margin: 0;padding: 0;">${query}</pre>
</p>


<h5 style="margin: 20px 0;padding: 0;font-size: 16px;line-height: 1.25;">Plazo de conservación de la descarga</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  La información relativa a esta descarga siempre estará disponible en <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
  y <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  El fichero <#if download.request.format == "SIMPLE_CSV">simple tab-separated values (TSV)<#else>${download.request.format}</#if> se guardará durante 6 meses (hasta ${download.eraseAfter?date}).  Puede solicitarnos guardar el archivo por más tiempo en <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Si cita esta descarga a través del DOI, lo detectaremos y guardaremos este fichero indefinidamente.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Para más información, visite <a href="${portal}faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #4ba2ce;text-decoration: none;">${portal}faq/?question=for-how-long-will-does-gbif-store-downloads</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Información / Preguntas frecuentes</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Si necesita ayuda para abrir los ficheros descargados, vea
  <a href="${portal}faq?question=opening-gbif-csv-in-excel" style="color: #4ba2ce;text-decoration: none;">${portal}faq?question=opening-gbif-csv-in-excel</a>
  o la sección de preguntas frecuentes en la página web de GBIF:
  <a href="${portal}faq" style="color: #4ba2ce;text-decoration: none;">${portal}faq</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>La Secretaría de GBIF</em>
</p>

<#include "footer.ftl">
