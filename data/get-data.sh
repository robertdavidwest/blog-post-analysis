#!/bin/sh
set -e

DIR=$( dirname "$0" )
DIR=$( cd "$DIR"; pwd )

FILE_JSONL="lJmU7N0dQ1.gz?download=posts.jsonl.gz"

rm -rf "$DIR/data-jsonl"
mkdir "$DIR/data-jsonl"

echo "Downloading data to $DIR/data-jsonl"
curl -L -o "$DIR/data-jsonl/posts.jsonl.gz" "https://cldup.com/$FILE_JSONL"
echo "Download complete"
