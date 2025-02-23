package com.search.demo;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.BlockingQueue;

public class FileCrawler implements Runnable {
    // directory where data files exist
    private final Path directory;
    // files to be indexed
    private final BlockingQueue<Path> fileQueue;
    // done with indexing
    private boolean done = false;

    public FileCrawler(Path directory, BlockingQueue<Path> fileQueue) {
        this.directory = directory;
        this.fileQueue = fileQueue;
    }

    public void start() {
        new Thread(this).start();
    }

    public void run() {
        final int[] files = {0};
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        fileQueue.put(file);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    files[0]++;

                    return super.visitFile(file, attrs);
                }
            });
        } catch (IOException e) {
            done = true;
            throw new RuntimeException(e);
        }

        System.out.println("Files found: " + files[0]);
        done = true;
    }

    public boolean isDone() {
        return done;
    }
}