package com.search.demo;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IndexingManager {

    private static final long MAX_TASK_QUEUE_SIZE = 10000;
    private final FileCrawler crawler;
    private final IndexWriter writer;
    private final Path indexDirectory;

    // files to be indexed
    private final BlockingQueue<Path> fileQueue;
    private final ExecutorService executorService;
    private AtomicLong startTime;
    public static AtomicInteger indexed = new AtomicInteger(0);

    public IndexingManager(Path dataDirectory, Path indexDirectory, int queueCapacity, int threadPoolSize) throws IOException {
        fileQueue = new LinkedBlockingQueue<>(queueCapacity);
        this.crawler = new FileCrawler(dataDirectory, fileQueue);
        this.indexDirectory = indexDirectory;
        Directory index = FSDirectory.open(indexDirectory);
        IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        this.writer = new IndexWriter(index, iwc);
        executorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    public void startIndexing() throws InterruptedException, IOException {
        startTime = new AtomicLong(System.currentTimeMillis());
        // start crawling the directory for files and adding to queue
        crawler.start();
        // keep an eye on indexing and clean up after
        indexingCleanupThread.start();
        // submit jobs to index files and limit jobs in queue
        long filesIndexing = 0;
        long current = System.currentTimeMillis();
        while (!isIndexingComplete()) {
            Path fileToIndex = fileQueue.poll(1, TimeUnit.SECONDS);
            if (fileToIndex == null) {
                continue;
            }

            FileIndexer fileIndexer = new FileIndexer(writer, fileToIndex);
            // System.out.println("sending file: " + fileToIndex);
            while (getPendingTaskQueueSize() > MAX_TASK_QUEUE_SIZE) {
                Thread.sleep(100);
            }
            executorService.submit(fileIndexer::index);
            filesIndexing++;
            if (filesIndexing % 1000 == 0) {
                System.out.printf("dispatched %d files, time: %d\n", filesIndexing, System.currentTimeMillis() - current);
                current = System.currentTimeMillis();
            }
        }

        System.out.println("Files submitted for indexing: " + filesIndexing);
    }

    private long getPendingTaskQueueSize() {
        return ((ThreadPoolExecutor) executorService).getQueue().size();
    }

    // indexing is complete if all files have been found and executor has consumed all tasks from queue
    private boolean isIndexingComplete() {
        return crawler.isDone() && fileQueue.isEmpty();
    }

    public MessageSearcher getMessageSearcher() throws IOException, InterruptedException {
        indexingCleanupThread.join();
        IndexReader reader = DirectoryReader.open(FSDirectory.open(indexDirectory));
        IndexSearcher searcher = new IndexSearcher(reader);
        return new MessageSearcher(searcher);
    }

    // wait for jobs to finish, shutdown executor service and close index writer
    private final Thread indexingCleanupThread = new Thread() {
        @Override
        public void run() {
            while (!isIndexingComplete()) {
                try {
                    sleep(1000);
                    System.out.println("still indexing. files indexed: " + indexed + " queue size: " + ((ThreadPoolExecutor) executorService).getQueue().size());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                executorService.shutdown();

                try {
                    while (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                        System.out.println("waiting for jobs to finish. files indexed: " + indexed + " queue size: " + ((ThreadPoolExecutor) executorService).getQueue().size());
                    }
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }

                System.out.printf("Indexed %d files in: %d ms \n",
                        IndexingManager.indexed.get(), System.currentTimeMillis() - startTime.get());
                try {
                    System.out.println("Closing writer");
                    writer.close();
                    System.out.println("Indexing complete");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    };
}
