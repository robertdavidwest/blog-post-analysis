Pre-Test Data
=============

This dataset consists of a collection of JSON documents. Each document represents a public blog post. There are about 1.8 million total posts. All of this data is data that is publicly available on the web as they are all public sites.

Schema
------

The schema for each JSON document is as follows:

```
{
    "blog_id": id identifying the blog
    "post_id": id identifying the post on that blog
    "lang": the language code for the post (this is a combination of the user setting and our language detection algorithm)
    "url": url to the post sans scheme (i.e. 'http(s)://')
    "date_gmt": date and time that the post was published (as set by the user). example: "2010-01-30 14:48:55"
    "title": text title of the post
    "content": text of the post with all html stripped out
    "author": authors displayed name
    "author_login": authors login username
    "author_id": user id of the author
    "liker_ids": array of user ids for people who have liked this post
    "like_count": number of likes for this post
    "commenter_ids": array of user ids for people who have commented on this post
    "comment_count": number of comments on this post
}
```

Data Formats
------------

You can download the data as a gzipped file (`posts.jsonl.gz` (1.3GB)) with one document per line, a.k.a. [JSON Lines](http://jsonlines.org).

You may use the included `get-data.sh` script to download. Otherwise the data is also available [on Cloudup](https://cloudup.com/c0G1FwY36l5).
