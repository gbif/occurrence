<#-- @ftlvariable name="" type="org.gbif.occurrence.mail.DownloadTemplateDataModel" -->
<#include "header.ftl">

<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">${download.request.creator} 様</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  あなたのダウンロードは以下のアドレスで入手できます:
  <br>
  <a href="${download.downloadLink}" style="color: #4ba2ce;text-decoration: none;">${download.downloadLink}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">引用</h5>
<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  このデータセット <strong>を使用する場合は、以下の引用を使用してください:</strong>
</p>
<p style="background: rgba(190, 198, 206, 0.25);margin: 0 0 20px;padding: 10px;line-height: 1.65;">
  GBIF.org (${downloadCreatedDateDefaultLocale}) GBIF Occurrence Download <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">ダウンロード情報</h5>
<p style="margin: 0;padding: 0;line-height: 1.65;">
  DOI: <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
 （有効になるまで数時間かかることがあります）
<br>
  作成日: ${download.created?datetime}
<br>
  レコード数: ${download.numberDatasets!0} 件のデータセットから ${download.totalRecords} 件
<br>
  圧縮データサイズ: ${size}
<br>
  ダウンロードフォーマット: <#if download.request.format == "SIMPLE_CSV">タブ区切り形式（TSV）<#else>${download.request.format}</#if>
<br>
  使用されたフィルタ:
  <pre style="white-space: pre-wrap;margin: 0;padding: 0;">${query}</pre>
</p>


<h5 style="margin: 20px 0;padding: 0;font-size: 16px;line-height: 1.25;">ダウンロードファイルの保持</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  このダウンロードに関する情報は、常に <a href="${download.doi.getUrl()}" style="color: #4ba2ce;text-decoration: none;">${download.doi.getUrl()}</a>
  と <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}オカレンス/ダウンロード/${download.key}</a> で入手できます。
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <#if download.request.format == "SIMPLE_CSV">シンプルなタブ区切り形式 (TSV)<#else>${download.request.format}</#if> ファイルは 6 か月間保存されます (${download.eraseAfter?date}まで)。  ファイル保持期間の延長を依頼することができます。 <a href="${portal}occurrence/download/${download.key}" style="color: #4ba2ce;text-decoration: none;">${portal}occurrence/download/${download.key}</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  DOIを用いてこのダウンロードを引用した場合、こちらで検出しファイルを無期限に保持します。
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  詳細については、 <a href="${portal}faq/?question=for-how-long-will-does-gbif-store-downloads" style="color: #4ba2ce;text-decoration: none;">${portal}faq/?question=for-how-long-will-does-gbif-store-downloads</a> を参照してください。
</p>


<h5 style="margin: 0 0 20px;padding: 0;font-size: 16px;line-height: 1.25;">情報 / FAQ</h5>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  ダウンロードしたファイルの開き方については、 
 <a href="${portal}faq?question=opening-gbif-csv-in-excel" style="color: #4ba2ce;text-decoration: none;">${portal}faq?question=opening-gbif-csv-in-excel</a> 
 またはGBIFウェブサイトのFAQを参照してください:
 <a href="${portal}faq" style="color: #4ba2ce;text-decoration: none;">${portal}faq</a>
</p>

<p style="margin: 0 0 20px;padding: 0;line-height: 1.65;">
  <em>GBIF事務局</em>
</p>

<#include "footer.ftl">
