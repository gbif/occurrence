<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="padding: 0;margin-bottom: 16px;line-height: 1.65;">
  ${download.request.creator}，您好！
</h5>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  很抱歉，处理您的下载时发生错误。
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  请通过 <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}事件/下载/${download.key}</a>了解更多详情，通过 <a href="${portal}health" style="color: #4ba2ce;text-decoration: none;">${portal}健康</a> 查看GBIF系统的当前状态，几分钟后再试。
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  如果问题仍然存在，请使用网站上的反馈系统或者通过 <a href="mailto:helpdesk@gbif.org" style="color: #4ba2ce;text-decoration: none;">helpdesk@gbif.org</a>联系我们。  请包含下载失败的下载密钥 (${download.key})。
</p>

<p style="padding: 0;margin-bottom: 20px;line-height: 1.65;">
  <em>GBIF 秘书处</em>
</p>

<#include "footer.ftl">
