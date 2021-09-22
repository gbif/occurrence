<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Bonjour ${download.request.creator},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Votre téléchargement est disponible à l'adresse suivante :
  <br>
  <a href="${download.downloadLink}" style="color: #4ba2ce;text-decoration: none;">${download.downloadLink}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Citation</h5>
<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Lorsque vous utilisez ce jeu de données <strong>veuillez utiliser la citation suivante :</strong>
</p>
<p style="background: rgba(190, 198, 206, 0.25);margin: 0 0 20px;padding: 10px;line-height: 1.65;">
  GBIF.org (${downloadCreatedDateDefaultLocale}) GBIF Occurrence Download <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Informations sur le Téléchargement</h5>
<p style="margin: 0;padding: 0;line-height: 1.65;">
  DOI: <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
  (ça peut prendre quelques heures avant qu'il soit actif)
<br>
 Date de création: ${download.created?datetime}
<br>
 Enregistrements inclus: ${download.totalRecords} enregistrements de  ${download.numberDatasets!0} jeux de données publiés
<br>
  Taille des données compressées: ${size}
<br>
Format de téléchargement: <#if download.request.format == "SIMPLE_CSV">simple tab-separated values (TSV)<#else>${download.request.format}</#if>
<br>
  Filtres utilisés :
  <pre style="white-space: pre-wrap;margin: 0;padding: 0;">${query}</pre>
</p>


<h5 style="margin: 20px 0;padding: 0;font-size: 16px;line-height: 1.25;">Conservation des fichiers téléchargés</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Les informations à propos de ce téléchargement seront toujours disponibles à <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
  et <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  The <#if download.request.format == "SIMPLE_CSV">simple tab-separated values (TSV)<#else>${download.request.format}</#if> file will be kept for six months (until ${download.eraseAfter?date}).  Vous pouvez nous demander de conserver le fichier plus longtemps sur <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Si vous citez ce téléchargement en utilisant le DOI, nous détecterons généralement cela et conserverons le fichier indéfiniment.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  For help with opening downloaded files, see
  <a href="${portal}faq?question=opening-gbif-csv-in-excel" style="color: #509E2F;text-decoration: none;">${portal}faq?question=opening-gbif-csv-in-excel</a>
  or the FAQ section of the GBIF website:
  <a href="${portal}faq" style="color: #509E2F;text-decoration: none;">${portal}faq</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Information / FAQ</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  For more information on this, see <a href="${portal}faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #509E2F;text-decoration: none;">${portal}faq/?question=for-how-long-will-does-gbif-store-downloads</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>Le Secrétariat du GBIF</em>
</p>

<#include "footer.ftl">
