#!/bin/zsh -e
#
# Supplement (and convert) a GBIF DWCA format download with images.
#

setopt -o pipefail

download_key=$1

[[ -d $download_key ]] && rm -Rf $download_key

wget -O $download_key.zip http://api.gbif.org/v1/occurrence/download/request/$download_key.zip

unzip -d $download_key $download_key.zip

cd $download_key

mkdir -p images
mkdir -p thumbnails

echo "gbifid	identifier	ml_size	thumb_size" > image-names

wc -l multimedia.txt

N=20
i=0
tail -n +2 multimedia.txt | cut -d$'\t' -f 1,4 | while IFS=$'\t' read -r id url; do
  ((i%N==1)) && wait || :
  ((i++)) || :
  echo $id $url
  number=$RANDOM
  name=images/${id}_$number.jpg
  thumb_name=thumbnails/${id}_${number}_t.jpg
#  echo "$id	$url	$name	$thumb_name"
  if [[ $url =~ '^http' && $url =~ 'g$' ]]; then
    curl -Ss --fail -o       $name --max-time 60 'http://api.gbif.org/v1/image/unsafe/fit-in/320x320/filters:format(jpg)/'$url && \
    curl -Ss --fail -o $thumb_name --max-time 60   'http://api.gbif.org/v1/image/unsafe/fit-in/64x64/filters:format(jpg)/'$url && \
    echo "$id	$url	$name	$thumb_name" >> image-names ||
    echo "$id	$url		" >> image-names
  else
    echo "$id	$url		" >> image-names
  fi
done | nl

mv multimedia.txt multimedia-original.txt

echo "gbifID	type	format	identifier	furtherInformationURL	title	description	CreateDate	creator	contributor	providerLiteral	audience	source	rights	Owner	Image	Thumbnail" > multimedia.txt

paste multimedia-original.txt <(cut -d$'\t' -f 3,4 image-names) | tail -n +2 >> multimedia.txt

[[ -e ../$download_key-images.zip ]] && rm -f $download_key-images.zip
zip -r ../$download_key-images.zip *.xml citations.txt multimedia.txt occurrence.txt rights.txt verbatim.txt dataset images thumbnails
