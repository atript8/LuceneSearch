package com.search.demo;

import org.apache.lucene.search.ScoreDoc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Scanner;

public class Indexer {

    private static final int pageSize = 10;
    private static final int numberOfIndexingThreads = 8;
    private static final int queueCapacity = 20;
    private MessageSearcher messageSearcher;

    public Indexer() throws Exception {
        Path dataPath = new File("/Volumes/mac-ext/IdeaProjects/masc_500k_texts").toPath();
        Path indexPath = new File("/Volumes/mac-ext/IdeaProjects/index").toPath();

        IndexingManager manager = new IndexingManager(dataPath, indexPath, queueCapacity, numberOfIndexingThreads);
        manager.startIndexing();

        messageSearcher = manager.getMessageSearcher();
    }

    public ScoreDoc[] search(String[] tokens) throws IOException {
        return messageSearcher.search(Arrays.asList(tokens), pageSize);
    }

    public static void main(String[] args) throws Exception {
        Indexer indexer = new Indexer();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("******************\nEnter a search string (type /q to quit): ");

            String userInput = scanner.nextLine();

            if (userInput.equalsIgnoreCase("/q")) {
                break;
            }
            if (userInput.isEmpty()) {
                continue;
            } else {
                String[] tokens = splitStringIntoArray(userInput);
                indexer.search(tokens);
            }
        }

        scanner.close();
    }

    private static String[] splitStringIntoArray(String str) {
        return str.split("\\s+");
    }
}