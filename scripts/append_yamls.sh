#!/bin/bash

outfile=$1
dir=$2
for f in $dir/*.yaml
do
	section="$(basename $f)"
	escapedf=$(printf '%s\n' "$f" | sed 's:/:\\&:g')
	sed -i "s/$escapedf/#$section/g" $outfile
	printf "\n# ${section}\n\`\`\`yaml\n$(cat $f)\n\`\`\`\n\n" >> $outfile
done
