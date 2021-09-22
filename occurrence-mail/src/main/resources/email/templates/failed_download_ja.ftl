<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="padding: 0;margin-bottom: 16px;line-height: 1.65;">
  ${download.request.creator} 様
</h5>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  申し訳ございませんが、ダウンロードの処理中にエラーが発生しました。
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  詳細については <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a> を、 GBIF.orgの現在のシステム正常性については<a href="${portal}health" style="color: #4ba2ce;text-decoration: none;">${portal}health</a> をご確認いただき、時間をおいてから再度お試しください。
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  問題が解決しない場合は、ウェブサイトのフィードバック、または <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a> までお問い合わせください。  失敗したダウンロードのダウンロードキー (${download.key}) もお知らせください。
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>GBIF事務局</em>
</p>

<#include "footer.ftl">
