<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.MultipleDownloadsTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Hola ${name},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <strong>Sus descargas de registros biológicos de GBIF listadas abajo están programadas para ser borradas en ${deletionDate?date}. </strong>. Si desea que mantengamos una descarga disponible, visite la página de descargas y haga clic en "Posponer la eliminación".
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Si los datos de una descarga se han utilizado en una publicación (artículo de revista, tesis, etc.), "Cuéntanos sobre el uso" haciendo clic en el botón en cada página de descarga.
  <em>No tenemos conocimiento de ningún trabajo publicado que utilice estas descargas.</em>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  GBIF mantiene las descargas de los usuarios durante 6 meses, tras los cuales pueden ser eliminadas.
  Cuando se elimina una descarga, se borra el archivo CSV o Darwin Core Archive, pero se conserva la página de descarga que muestra la consulta y los conjuntos de datos utilizados en la descarga.
  El DOI también se mantiene, y es la forma preferida de citar las descargas.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Las descargas de GBIF utilizadas en una publicación se mantendrán indefinidamente.
</p>

<ul style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
<#list downloads as d>
  <li><p>DOI: <a href="${d.download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${d.download.doi.getUrl()}</a>
    <br>
    Página de descarga: <a href="${portal}occurrence/download/${d.download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${d.download.key}</a>
    <br>
    Hora de la descarga solicitada: ${d.download.created?datetime}
    <#if d.download.totalRecords &gt; 0>
    <br>
    Número de registros biológicos: ${d.download.totalRecords?string.number} de ${(d.download.numberDatasets!0)?string.number} conjuntos de datos publicados
    </#if>
  </p></li>
</#list>
</ul>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Por favor, póngase en contacto con <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a> si tiene preguntas sobre el contenido de este correo electrónico, o consulte la sección <a href="${portal}faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #4ba2ce;text-decoration: none;">FAQ</a>.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Para ver todas sus descargas de GBIF, visite <a href="${portal}user/download" style="color: #4ba2ce;text-decoration: none;">${portal}usuario/descarga</a>.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>Secretaría de GBIF</em>
</p>

<#include "footer.ftl">
