package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main
{
    private static final int THREAD_POOL_SIZE = 10; // Adjustable based on system and hardware limitations
    private static final int MAX_TIMEOUT = 60; // minutes

    private static final String CHUNKIFY_FILE_NAME_PATTERN = "chunk_%d.csv";
    private static final String MERGE_FILE_NAME_PATTERN = "%d_chunk_%d.csv";

    private static final Logger logger = LogManager.getLogger(Main.class);
    private static final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public static void main(String[] args) throws Exception
    {
        String inputFileName = args[0];
        String outputFileName = args[1];
        String maxLinesInMemory = args[2];

        sort(inputFileName, outputFileName, Integer.parseInt(maxLinesInMemory));
    }

    private static void sort(String inputFileName, String outputFileName, int maxLinesInMemory) throws Exception
    {
        List<String> chunkFiles = chunkify(inputFileName, maxLinesInMemory);
        mergeSortedChunks(chunkFiles, outputFileName, maxLinesInMemory, 0);
    }

    /**
     * Divides the large file into smaller chunks which are sorted ascending
     * @param inputFilePath
     * @param maxLinesInMemory maximum number of lines that can be held in memory at any given time
     * @return List of the sorted chunk files
     * @throws Exception in case chunkify fails
     */
    private static List<String> chunkify(String inputFilePath, int maxLinesInMemory) throws Exception
    {
        List<String> chunkFiles = new LinkedList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath)))
        {
            String line;
            int chunkNumber = 0;
            int linesNumber = 0;
            List<String> lines = new LinkedList<>();
//            List<Future<Boolean>> results = new LinkedList<>();

            while ((line = reader.readLine()) != null)
            {
                lines.add(line);
                linesNumber++;

                if (linesNumber == maxLinesInMemory)
                {
                    String chunkFileName = String.format(CHUNKIFY_FILE_NAME_PATTERN, chunkNumber);
                    sortChunk(new ArrayList<>(lines), chunkFileName);
                    chunkFiles.add(chunkFileName);
   /*                 Future<Boolean> success = executorService.submit(() ->
                    {
                        try
                        {
                            sortChunk(new ArrayList<>(lines), chunkFileName);
                            chunkFiles.add(chunkFileName);
                            return true;
                        }
                        catch (Exception exception)
                        {
                            logger.error("sortChunk() method failed", exception);
                            return false;
                        }
                    });*/

//                    results.add(success);
                    lines.clear();
                    chunkNumber++;
                    linesNumber = 0;
                }
            }

            if (!lines.isEmpty())
            {
                String chunkFileName = String.format(CHUNKIFY_FILE_NAME_PATTERN, chunkNumber);
                sortChunk(lines, chunkFileName);
                chunkFiles.add(chunkFileName);
            }

//            boolean terminated = executorService.awaitTermination(MAX_TIMEOUT, TimeUnit.MINUTES);
//
//            if (!terminated)
//                throw new Exception("Chunkify threads did not terminate successfully");
//
//            for (Future<Boolean> result : results)
//            {
//                if (!result.get())
//                    throw new Exception("Sort chunk has failed");
//            }
        }

        return chunkFiles;
    }

    private static void sortChunk(List<String> chunk, String chunkFileName) throws Exception
    {
        chunk.sort(Comparator.comparingInt(Integer::parseInt));

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(chunkFileName)))
        {
            for (String line : chunk)
            {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    /**
     * Recursively merges smaller sorted chunks into bigger smaller chunks until the whole file is sorted
     * @param chunkFiles the chunk files to be sorted
     * @param outputFileName the name of the output file in which the sorted data will be written to
     * @param maxLinesInMemory maximum number of lines that can be held in memory at any given time
     * @throws Exception in case the merge operation fails
     */
    private static void mergeSortedChunks(List<String> chunkFiles, String outputFileName, int maxLinesInMemory, int level) throws Exception
    {
        int counter = 0;
        int chunkNumber = 0;

        List<String> _chunkFiles = new LinkedList<>();
        List<String> mergedChunkFiles = new LinkedList<>();
        Iterator<String> iterator = chunkFiles.iterator();

        if (chunkFiles.size() == 1)
        {
            Path source = Paths.get(chunkFiles.get(0));
            Files.move(source, source.resolveSibling(outputFileName));
            return;
        }

        while (iterator.hasNext())
        {
            String chunkFile = iterator.next();

            _chunkFiles.add(chunkFile);
            iterator.remove();
            counter++;

            if (counter == maxLinesInMemory)
            {
                String mergedFileName = String.format(MERGE_FILE_NAME_PATTERN, level, chunkNumber);
                doMergeSortedChunks(_chunkFiles, mergedFileName);
                mergedChunkFiles.add(mergedFileName);
                _chunkFiles.clear();
                counter = 0;
                chunkNumber++;
            }
        }

        if (!_chunkFiles.isEmpty())
        {
            String mergedFileName = String.format(MERGE_FILE_NAME_PATTERN, level, chunkNumber);
            doMergeSortedChunks(_chunkFiles, mergedFileName);
            mergedChunkFiles.add(mergedFileName);
        }

        if (!mergedChunkFiles.isEmpty())
            mergeSortedChunks(mergedChunkFiles, outputFileName, maxLinesInMemory, level + 1);
    }

    private static void doMergeSortedChunks(List<String> chunkFiles, String outputFileName) throws Exception
    {
        try
        {
            Map<String, BufferedReader> readers = new HashMap<>(); // file name -> buffered reader
            PriorityQueue<LineFileDescriptor> minHeap = new PriorityQueue<>(); // heap of (line, file name)

            for (String file : chunkFiles)
            {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                readers.put(file, reader);
                String line = reader.readLine();
                minHeap.add(new LineFileDescriptor(line, file));
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName)))
            {
                while (!minHeap.isEmpty())
                {
                    LineFileDescriptor min = minHeap.poll();
                    writer.write(min.getLine());
                    writer.newLine();

                    // Read the next line from the same file
                    BufferedReader reader = readers.get(min.getFileName());
                    String line = reader.readLine();

                    if (line != null)
                        minHeap.add(new LineFileDescriptor(line, min.getFileName()));
                }
            }

            for (String tempFile : chunkFiles)
            {
                readers.get(tempFile).close();
                Files.delete(Path.of(tempFile));
            }

        }
        catch (Exception exception)
        {
            logger.error("mergeChunkFiles() failed");
            throw exception;
        }
    }
}