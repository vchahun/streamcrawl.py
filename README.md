streamcrawl.py
==============

An interactive crawler for the Twitter streaming API, using [tweetstream](http://pypi.python.org/pypi/tweetstream)

Usage
-----

Create a configuration file in JSON format:
```json
{
    "user": "USERNAME",
    "password": "PASSWORD",
    "tags": ["tag1", "tag2", "...", "tagn"]
}
```

Run: `python tweetstream.py config.json output.bz2`
