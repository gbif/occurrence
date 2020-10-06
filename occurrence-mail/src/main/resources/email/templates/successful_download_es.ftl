<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">Hola ${download.request.creator},</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Su archivo de descarga se encuentra disponible en la siguiente dirección:
  <br>
  <a href="${download.downloadLink}" style="color: #509E2F;text-decoration: none;">${download.downloadLink}</a>
</p>


<h4 style="margin: 0 0 20px;padding: 0;font-size: 20px;line-height: 1.25;">Citas</h4>
<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Al referenciar este archivo <strong> favor usar la siguiente cita:</strong>
</p>
<p style="background: #e8e8e8;margin: 0 0 20px;padding: 10px;line-height: 1.65;">
  GBIF.org (${downloadCreatedDateDefaultLocale}) GBIF Occurrence Download <a href="${download.doi.getUrl()}" style="color: #509E2F;text-decoration: none;">${download.doi.getUrl()}</a>
</p>


<h4 style="margin: 0 0 20px;padding: 0;font-size: 20px;line-height: 1.25;">Información del archivo de descarga</h4>
<p style="margin: 0;padding: 0;line-height: 1.65;">
  DOI: <a href="${download.doi.getUrl()}" style="color: #509E2F;text-decoration: none;">${download.doi.getUrl()}</a>
  (podría tardar algunas es activarse)
<br>
  Fecha de creación: ${download.created?datetime}
<br>
  Registros incluidos: ${download.totalRecords} registros de ${download.numberDatasets!0} conjuntos de datos publicados
<br>
  Tamaño de los datos, comprimidos: ${size}
<br>
  Formato: <#if download.request.format == "SIMPLE_CSV">simple separado por el caracter TAB (TSV)<#else>${download.request.format}</#if>
<br>
  Filtro utilizado:
  <pre style="white-space: pre-wrap;margin: 0;padding: 0;">${query}</pre>
</p>


<h4 style="margin: 20px 0;padding: 0;font-size: 20px;line-height: 1.25;">Política de rentención de archivos de descarga</h4>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  La información acerca de esta descarga estará siempre disponible en <a href="${download.doi.getUrl()}" style="color: #509E2F;text-decoration: none;">${download.doi.getUrl()}</a>
  y en <a href="${portal}occurrence/download/${download.key}" style="color: #509E2F;text-decoration: none;">${portal}occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Los archivos <#if download.request.format == "SIMPLE_CSV">simples separados por el caracter TAB (TSV)<#else>${download.request.format}</#if> serán almacenados por un período seis meses (hasta el ${download.eraseAfter?date}).<br>
  Puede solicitarnos extender dicho período desde <a href="${portal}occurrence/download/${download.key}" style="color: #509E2F;text-decoration: none;">${portal}occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Si el archivo de descarga es citado utilizando el DOI, dicha referencia es usualmente detectada y el archivo mantenido indefinidamente.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Para más información acerca de esto, ver <a href="${portal}faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #509E2F;text-decoration: none;">${portal}faq/?question=for-how-long-will-does-gbif-store-downloads</a>
</p>


<h4 style="margin: 0 0 20px;padding: 0;font-size: 20px;line-height: 1.25;">Información / FAQ</h4>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Para obtener ayuda de cómo abrir archivos de descarga, ver
  <a href="${portal}faq?question=opening-gbif-csv-in-excel" style="color: #509E2F;text-decoration: none;">${portal}faq?question=opening-gbif-csv-in-excel</a>
  ó consultar la sección FAQ en el sitio web de GBIF
  <a href="${portal}faq" style="color: #509E2F;text-decoration: none;">${portal}faq</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>Secretariado de GBIF</em>
</p>

<#include "footer.ftl">
