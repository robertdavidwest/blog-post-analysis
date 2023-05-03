# An Analysis of Blog Posts using Spark and Scala

I am working with Spark 3.x to analyse blog data (and write associated unit tests)

## Prerequisites

You will need to have the following setup in your local environment or wherever you choose to run this code:

- JDK
- sbt
- At least 4GB of RAM

## Setup

- Clone this repo
- Run [this script](data/get-data.sh) to download the dataset (1.3G gziped)

To run the blog analysis, `cd` into the app directory and run :

```
$ bash run.sh
```

By default a full data run will take place, to just use a sample of 100 rows add the flag `--sample`

```
$ bash run.sh --sample
```

By default the spark sql datastore created will be deleted at the end of the run to make develement easier. If you want to keep the datastore add the flag `--no-cleanup`

```
$ bash run.sh --no-cleanup
```

- The script will produce a summary file in markdown.
- To run the units tests run the command `$ sbt "test"` from the app directory.

The Job will answer the following questions:

### Posts Per Blog

- Minimum number of posts in a blog
- Mean number of posts in a blog
- Maximum number of posts in a blog
- Total number of blogs in the dataset
- Std Deviation of posts in a blog
- Mean posts per blog per year

### Who Likes Posts?

The script will show the percentage of likes coming from people who were NOT authors of blogs themselves.

### Writing a Table

We also want to load this dataset into a Hive table so that we can do ad-hoc queries on it in the future. We will do this using Spark to write out some files that can be loaded into HDFS. To keep things simple we're going to keep values in each column as flat scalars: columns like `liker_ids` can be simple delimited strings.

It is expected that the read load on this table is going to be much more than write load. As such we want the table to be optimized for querying even at the expense of more data processing during writes. Here are a bunch of example queries that we would like to run on such a table:

```
SELECT count(\*) AS posts, lang
FROM dataset
WHERE date_gmt BETWEEN '2010-01-01 00:00:00' AND '2010-12-31 23:59:59'
GROUP BY lang
```

```
SELECT sum(comment_count) AS comments
FROM dataset
WHERE
lang = 'en' AND
date_gmt BETWEEN '2012-01-01' AND '2012-01-31'
```

```
SELECT sum(like_count) / sum(comment_count) AS ratio
FROM dataset
```

We also want the full dataset loaded in the table. All data is still needed, not just the results of these queries.
