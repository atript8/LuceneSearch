package com.search.demo;

import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileIndexer {
    private final IndexWriter writer;
    // file to index
    private final Path filePath;

    public FileIndexer(IndexWriter writer, Path filePath) throws IOException {
        this.writer = writer;
        this.filePath = filePath;
    }

    public void index() {
        //System.out.println("Indexing file: " + filePath);
        InputStream inputStream = null;
        try {
            inputStream = Files.newInputStream(filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String text;
            while ((text = reader.readLine()) != null) {
                Document document = new Document();
                document.add(new StringField("file", filePath.toFile().getAbsolutePath(), Field.Store.YES));
                document.add(new TextField("raw", text, Field.Store.YES));
                writer.addDocument(document);
            }
            IndexingManager.indexed.addAndGet(1);
            System.out.println("Indexed file: " + filePath.toFile().getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
