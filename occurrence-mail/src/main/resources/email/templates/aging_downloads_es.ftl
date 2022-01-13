<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.MultipleDownloadsTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Hola ${name},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <strong>Sus descargas de registros biológicos de GBIF listadas abajo están programadas para ser borradas en ${deletionDate?date}. </strong>. Si desea que mantengamos una descarga disponible, visite la página de descargas y haga clic en "Posponer la eliminación".
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  If data from a download have been used in a publication (journal article, thesis etc.) please inform us by clicking the button “Tell us about usage” on each download page.
  <em>We are not aware of any published work using these downloads.</em>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  GBIF keep user downloads for 6 months, after which they may be deleted.
  When a download is deleted, the CSV or Darwin Core Archive file is erased, but the download page showing the query and datasets used in the download is retained.
  The DOI is also kept, and is the preferred way to cite downloads.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Las descargas de GBIF utilizadas en una publicación se mantendrán indefinidamente.
</p>

<ul style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
<#list downloads as d>
  <li><p>DOI: <a href="${d.download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${d.download.doi.getUrl()}</a>
    <br>
    Download page: <a href="${portal}occurrence/download/${d.download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${d.download.key}</a>
    <br>
    Time download requested: ${d.download.created?datetime}
    <#if d.download.totalRecords &gt; 0>
    <br>
    Number of occurrence records: ${d.download.totalRecords?string.number} from ${(d.download.numberDatasets!0)?string.number} published datasets
    </#if>
  </p></li>
</#list>
</ul>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Please contact <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a> if you have questions regarding the contents of this email, or refer to the <a href="${portal}faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #4ba2ce;text-decoration: none;">FAQ</a>.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Para ver todas sus descargas de GBIF, visite <a href="${portal}user/download" style="color: #4ba2ce;text-decoration: none;">${portal}usuario/descarga</a>.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>Secretaría de GBIF</em>
</p>

<#include "footer.ftl">
