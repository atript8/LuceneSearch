This is sample project to show how to use the lucene library to create a local document search functionality. I was trying to implement this and was frustrated with lack of examples and documentation of how to use lucene inspite of it being so widely used, so I am sharing it here. This could come in handy if solutions like elasticsearch/solr are an overkill for your use case. This can be implemented within the same application/jvm or as a separate service on the same vm to respond to search queries. 

Further improvements can be:
1. to expose a rest api
2. ability to read from s3 buckets

Usage:
just run demo/src/main/java/com/search/demo/Indexer.java and enter list of words to query the index on.