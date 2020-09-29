<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">Уважаемый/ая ${download.request.creator},</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Ваш скачиваемый файл доступен по следующему адресу:
  <br>
  <a href="${download.downloadLink}" style="color: #509E2F;text-decoration: none;">${download.downloadLink}</a>
</p>


<h4 style="margin: 0 0 20px;padding: 0;font-size: 20px;line-height: 1.25;">Цитирование</h4>
<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  При использовании этого набора данных <strong>используйте следующую ссылку:</strong>
</p>
<p style="background: #e8e8e8;margin: 0 0 20px;padding: 10px;line-height: 1.65;">
  GBIF.org (${downloadCreatedDateDefaultLocale}) GBIF Occurrence Download <a href="${download.doi.getUrl()}" style="color: #509E2F;text-decoration: none;">${download.doi.getUrl()}</a>
</p>


<h4 style="margin: 0 0 20px;padding: 0;font-size: 20px;line-height: 1.25;">Информация о загрузке</h4>
<p style="margin: 0;padding: 0;line-height: 1.65;">
  DOI: <a href="${download.doi.getUrl()}" style="color: #509E2F;text-decoration: none;">${download.doi.getUrl()}</a>
  (активация может занять несколько часов)
<br>
  Дата создания: ${download.created?datetime}
<br>
  Количество записей: ${download.totalRecords} записей из ${download.numberDatasets!0} опубликованных наборов данных
<br>
  Размер сжатых данных: ${size}
<br>
  Формат загрузки: <#if download.request.format == "SIMPLE_CSV">TSV (simple tab-separated values)<#else>${download.request.format}</#if>
<br>
  Использованный фильтр:
  <pre style="white-space: pre-wrap;margin: 0;padding: 0;">${query}</pre>
</p>


<h4 style="margin: 20px 0;padding: 0;font-size: 20px;line-height: 1.25;">Хранение файла загрузки</h4>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Информация об этой загрузке всегда будет доступна по адресам <a href="${download.doi.getUrl()}" style="color: #509E2F;text-decoration: none;">${download.doi.getUrl()}</a>
  и <a href="${portal}ru/occurrence/download/${download.key}" style="color: #509E2F;text-decoration: none;">${portal}ru/occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <#if download.request.format == "SIMPLE_CSV">TSV (simple tab-separated values)<#else>${download.request.format}</#if> файл, будет храниться в течение шести месяцев (до ${download.eraseAfter? date}). Вы можете попросить
  нас хранить файл дольше <a href="${portal}ru/occurrence/download/${download.key}" style="color: #509E2F;text-decoration: none;">${portal}ru/occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Если вы цитируете скачанные данные с помощью DOI, мы обычно это обнаруживаем и храним файл на неопределенный срок.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Дополнительные сведения см. <a href="${portal}ru/faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #509E2F;text-decoration: none;">${portal}ru/faq/?question=for-how-long-will-does-gbif-store-downloads</a>.
</p>


<h4 style="margin: 0 0 20px;padding: 0;font-size: 20px;line-height: 1.25;">Информация / FAQ</h4>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Для получения информация об открытии загружаемых файлов, см.
  <a href="${portal}ru/faq?question=opening-gbif-csv-in-excel" style="color: #509E2F;text-decoration: none;">${portal}ru/faq?question=opening-gbif-csv-in-excel</a>
  или раздел FAQ на сайте GBIF:
  <a href="${portal}ru/faq" style="color: #509E2F;text-decoration: none;">${portal}ru/faq</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>GBIF Секретариат</em>
</p>

<#include "footer.ftl">
