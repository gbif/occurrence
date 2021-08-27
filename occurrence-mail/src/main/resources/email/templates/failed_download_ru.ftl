<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Уважаемый пользователь, ${download.request.creator},
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Сожалеем, но при обработке скачиваемого файла произошла ошибка.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  См. <a href="${portal}ru/occurrence/download/${download.key}" style="color: #509E2F;text-decoration: none;">${portal}ru/occurrence/download/${download.key}</a> для получения более подробной информации, <a href="${portal}ru/health" style="color: #509E2F;text-decoration: none;">${portal}ru/health</a> для текущего состояния систем GBIF.org и повторите попытку через несколько минут.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  Если проблема не исчезнет, свяжитесь с нами, используя систему обратной связи на вебсайте или по адресу <a href="mailto:helpdesk@gbif.org" style="color: #509E2F;text-decoration: none;">helpdesk@gbif.org</a> Пожалуйста, укажите ключ неудавшегося скачивания (${download.key}).  Please include the download key (${download.key}) of the failed download.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>GBIF Секретариат</em>
</p>

<#include "footer.ftl">
