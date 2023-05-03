#!/bin/bash
sampleRun=false
cleanUp=true

dataFilepath="../data/data-jsonl/posts.jsonl.gz"
sampleDataFilepath="../data/data-jsonl/samplePosts100.jsonl"
sampleSize=100

fullOutpath="../ANSWERS_final.md"
sampleOutpath="../ANSWERS_sample100.md"

# Parse command line arguments
while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        --sample)
        sampleRun=true
        shift # past argument
        ;;
        --no-cleanup)
        cleanUp=false
        shift # past argument
        ;;
        *)
        # unknown option
        echo "Unknown option: $1"
        exit 1
        ;;
    esac
done

# For convenience in dev
# this script clears out the the metastore_db and spark-warehouse directories
# created by PostAnalysis
# So you can just run this script over and over while deving 
# and not have to worry about cleaning up the directories

if $sampleRun; then
  if [ ! -d "$sampleDataFilepath" ]; then
    echo "File not found, creating sample data"
    # create sample data
    sbt "runMain com.a8c.pretest.CreateSamplePosts $dataFilepath $sampleDataFilepath $sampleSize"
  fi 
  # sample run
  sbt "runMain com.a8c.pretest.PostAnalysis $sampleDataFilepath $sampleOutpath"

else
  # full run
  sbt "runMain com.a8c.pretest.PostAnalysis $dataFilepath $fullOutpath"
fi

if $cleanUp; then
  rm -r metastore_db                                                             
  rm derby.log
  rm -r spark-warehouse
fi