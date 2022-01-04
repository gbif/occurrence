<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.MultipleDownloadsTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Olá ${name},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <strong>Your GBIF occurrence downloads listed below are scheduled for deletion on ${deletionDate?date}.</strong>
  If you would like us to keep a download available, please visit the download page and click “Postpone Deletion”.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Se os dados de um download foram usados em uma publicação (artigo de periódico, tese, etc.) por favor nos informe clicando no botão "Conte-nos sobre o uso" em cada página de download.
  <em>Não temos conhecimento de qualquer trabalho publicado usando esses downloads.</em>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  O GBIF mantêm as informações baixadas pelo usuário por 6 meses, após o qual podem ser excluídas.
  When a download is deleted, the CSV or Darwin Core Archive file is erased, but the download page showing the query and datasets used in the download is retained.
  O DOI também é mantido e é a forma preferida de citar a informação baixada.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  GBIF downloads used in a publication will be kept indefinitely.
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
  Por favor, contate <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a> se você tiver dúvidas sobre o conteúdo deste e-mail ou consulte <a href="${portal}faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #4ba2ce;text-decoration: none;">FAQ</a>.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Para ver todos os seus downloads do GBIF, acesse <a href="${portal}user/download" style="color: #4ba2ce;text-decoration: none;">${portal}user/donwload</a>.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>Secretaria do GBIF</em>
</p>

<#include "footer.ftl">
