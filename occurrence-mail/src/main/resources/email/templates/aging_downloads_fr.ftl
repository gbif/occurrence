<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.MultipleDownloadsTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Bonjour ${name},</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <strong>Vos téléchargements d'occurrences GBIF listés ci-dessous sont programmés pour suppression le ${deletionDate?date}.</strong>
  Si vous souhaitez que nous conservions un téléchargement, veuillez visiter la page de téléchargement et cliquer sur « Reporter la suppression ».
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Si les données d'un téléchargement ont été utilisées dans une publication (article de revue, thèse, etc.). veuillez nous informer en cliquant sur le bouton « Parlez-nous de l'utilisation » sur chaque page de téléchargement.
  <em>Nous ne sommes au courant d'aucun travail publié utilisant ces téléchargements.</em>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Le GBIF conserve les téléchargements des utilisateurs pendant 6 mois, période après laquelle ils seront potentiellement supprimés.
  Lorsqu'un téléchargement est supprimé, le fichier CSV ou l'Archive Darwin Core sont effacés, mais la page de téléchargement montrant la requête et les jeux de données utilisés dans le téléchargement est conservée.
  Le DOI est également conservé, et est le moyen à privilégier pour citer des téléchargements.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Les téléchargements GBIF utilisés dans une publication seront conservés indéfiniment.
</p>

<ul style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
<#list downloads as d>
  <li><p>DOI : <a href="${d.download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${d.download.doi. etUrl()}</a>
    <br>
    Page de téléchargement : <a href="${portal}occurrence/download/${d.download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${d.download.key}</a>
    <br>
    Temps de téléchargement demandé : ${d.download.created?datetime}
    <#if d.download.totalRecords &gt; 0>
    <br>
    Nombre d'occurrences : ${d.download.totalRecords?string.number} de ${(d.download.numberDatasets!0)?string.number} jeux de données
    </#if>
  </p></li>
</#list>
</ul>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Veuillez contacter <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a> si vous avez des questions concernant le contenu de ce ce courriel, ou consultez la <a href="${portal}faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #4ba2ce;text-decoration: none;">FAQ</a>.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Pour voir tous vos téléchargements GBIF, visitez <a href="${portal}user/download" style="color: #4ba2ce;text-decoration: none;">${portal}utilisateur/téléchargement</a>.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>Le Secrétariat du GBIF</em>
</p>

<#include "footer.ftl">
