<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="padding: 0;margin-bottom: 16px;line-height: 1.65;">
  مرحبا ${download.request.creator}،
</h5>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  نحن آسفون، ولكن حدث خطأ أثناء معالجة التنزيل الخاص بك.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  الرجاء الاطلاع على <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}حدث/تنزيل/${download.key}</a> لمزيد من التفاصيل، <a href="${portal}system-health" style="color: #4ba2ce;text-decoration: none;">${portal}الصحة</a> للوضع الحالي لأنظمة GBIF.org، وحاول مرة أخرى في بضع دقائق.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  إذا استمرت المشكلة، اتصل بنا باستخدام نظام التعليقات على الموقع، أو على <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a>.  الرجاء تضمين مفتاح التحميل (${download.key}) من التنزيل الفاشل.
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>أمانة المرفق العالمي لمعلومات التنوع الحيوي</em>
</p>

<#include "footer.ftl">
