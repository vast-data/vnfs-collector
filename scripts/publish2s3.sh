#~/bin/bash
version=$(cat version.txt).g$(git rev-parse --short HEAD)

rpmfile=vnfs-collector-${version}.noarch.rpm
debfile=vnfs-collector_${version}_all.deb
echo s3cmd put dist/$rpmfile s3://vast-client-metrics/$rpmfile
s3cmd put dist/$rpmfile s3://vast-client-metrics/$rpmfile
echo s3cmd put dist/$debfile s3://vast-client-metrics/$debfile
s3cmd put dist/$debfile s3://vast-client-metrics/$debfile
if [ "$1" == "latest" ]; then
	meta=$(mktemp)
	echo "{'latest': '$version'}" > $meta
	echo "meta: `cat $meta`"
	echo s3cmd put meta.json s3://vast-client-metrics/meta.json
	s3cmd put $meta s3://vast-client-metrics/meta.json
fi


