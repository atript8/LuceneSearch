package com.search.demo;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanQuery.Builder;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class MessageSearcher {
    private IndexSearcher searcher;

    public MessageSearcher(IndexSearcher searcher) {
        this.searcher = searcher;
    }

    public ScoreDoc[] search(List<String> tokens, int pageSize) throws IOException {
        Builder queryBuilder = new Builder();
        for (String token: tokens) {
            queryBuilder.add(new TermQuery(new Term("raw", token)), BooleanClause.Occur.MUST);
        }
        Query query = queryBuilder.build();
        System.out.println("query: " + query);
        Long startSearch = System.currentTimeMillis();
        TopDocs docs = searcher.search(query, pageSize);
        System.out.printf("Found %d hits in %d ms.\n", docs.scoreDocs.length, System.currentTimeMillis() - startSearch);
        ScoreDoc[] hits = docs.scoreDocs;
        for (int i = 0; i < hits.length; ++i) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);
            System.out.println((i + 1) + ". In file:" + d.get("file") + "\n\t\"" + d.get("raw").trim() + "\"");
        }

        return hits;
    }
}