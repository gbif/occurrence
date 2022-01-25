<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Вітаємо, ${download.request.creator}</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Ваше завантаження доступне за посиланням:
  <br>
  <a href="${download.downloadLink}" style="color: #4ba2ce;text-decoration: none;">${download.downloadLink}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Як цитувати</h5>
<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Використовуючи цей набір, будь ласка, <strong>вживайте таку форму цитування:</strong>
</p>
<p style="background: rgba(190, 198, 206, 0.25);margin: 0 0 20px;padding: 10px;line-height: 1.65;">
  GBIF.org (${downloadCreatedDateDefaultLocale}) GBIF Occurrence Download <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Інформація про завантаження</h5>
<p style="margin: 0;padding: 0;line-height: 1.65;">
  DOI: <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
  (може потребувати кількох годин для активації)
<br>
  Дата створення: ${download.created?datetime}
<br>
  Включае записи: ${download.totalRecords} записів з ${download.numberDatasets!0} опублікованих наборів даних
<br>
  Розмір утиснутих даних: ${size}
<br>
  Формат завантаження: <#if download.request.format == "SIMPLE_CSV">текстовий файл, розділений знаками табуляції (TSV)<#else>${download.request.format}</#if>
<br>
  Використано фільтр:
  <pre style="white-space: pre-wrap;margin: 0;padding: 0;">${query}</pre>
</p>


<h5 style="margin: 20px 0;padding: 0;font-size: 16px;line-height: 1.25;">Зберігання завантаженого файлу</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Інформація про це завантаження завжди буде доступною в <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.Url()}</a>
  та <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}currence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <#if download.request.format == "SIMPLE_CSV">прості значення, розділені знаком табуляції (TSV)<#else>${download.request.format}</#if> зберігатимуться впродовж шести місяців (до ${download.eraseAfter?date}).  Ви можете попросити зберегти файл довше від <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Якщо ви цитуватимете це завантаження з використанням DOI, ми зазвичай реєструємо це та зберігатимемо файл у будь-якому разі.
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Для додаткової інформації, див. <a href="${portal}faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #4ba2ce;text-decoration: none;">${portal}faq/?question=for-how-long-will-does-gbif-store-downloads</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">Інформація / часті запитання</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  Щоб отримати допомогу з відкриванням завантажених файлів, див. 
  <a href="${portal}faq?question=opening-gbif-csv-in-excel" style="color: #4ba2ce;text-decoration: none;">${portal}faq?question=opening-gbif-csv-in-excel</a>
  або інші розділи Частих запитань на сайті GBIF:
  <a href="${portal}faq" style="color: #4ba2ce;text-decoration: none;">${portal}faq</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>Секретаріат GBIF</em>
</p>

<#include "footer.ftl">
