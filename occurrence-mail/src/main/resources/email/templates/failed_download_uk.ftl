<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="padding: 0;margin-bottom: 16px;line-height: 1.65;">
  Вітаємо, ${download.request.creator}
</h5>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Вибачте, але при обробці вашого завантаження сталася помилка.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Будь ласка, перегляньте подробиці на <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a>, а також перевірте <a href="${portal}system-health" style="color: #4ba2ce;text-decoration: none;">${portal}system-health</a> поточний стан системи GBIF.org, і спробуйте знову за кілька хвилин.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Якщо проблема залишається, зв'яжіться з нами за допомогою системи зворотного зв'язку на сайті або за посиланням <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a>.  Будь ласка, вкажіть ключ завантаження (${download.key}) невдалого завантаження.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>Секретаріат GBIF</em>
</p>

<#include "footer.ftl">
