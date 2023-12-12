package com.v.sort;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ExternalSort
{
    private static final int THREAD_POOL_SIZE = 10; // Adjustable based on system and hardware limitations
    private static final String CHUNKIFY_FILE_NAME_PATTERN = "chunk_%d.csv";
    private static final String MERGE_FILE_NAME_PATTERN = "%d_chunk_%d.csv";

    private static final Logger logger = LogManager.getLogger(ExternalSort.class);
    private static ExecutorService executorService;

    public static void main(String[] args) throws Exception
    {
        try
        {
            executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

            String inputFileName = args[0];
            String outputFileName = args[1];
            String maxLinesInMemory = args[2];

            sort(inputFileName, outputFileName, Integer.parseInt(maxLinesInMemory));
        }
        finally
        {
            executorService.shutdown();
        }
    }

    /**
     * Gets input file and sorts it ascending under the constraint of having 'maxLinesInMemory' lines in memory at any given time
     *
     * @param inputFileName    the name of the file to be sorted
     * @param outputFileName   the name of the output file in which the sorted data will be written to
     * @param maxLinesInMemory the maximum number of lines that can be held in memory at any given time
     * @throws Exception
     */
    private static void sort(String inputFileName, String outputFileName, int maxLinesInMemory) throws Exception
    {
        List<String> chunkFiles = chunkify(inputFileName, maxLinesInMemory);
        mergeSortedChunks(chunkFiles, outputFileName, maxLinesInMemory, 0);
    }

    /**
     * Divides the large file into smaller chunks which are sorted ascending internally
     *
     * @param inputFileName    the name of the file to be sorted
     * @param maxLinesInMemory maximum number of lines that can be held in memory at any given time
     * @return List of the chunk files
     * @throws Exception in case chunkify fails
     */
    private static List<String> chunkify(String inputFileName, int maxLinesInMemory) throws Exception
    {
        List<String> chunkFiles = Collections.synchronizedList(new LinkedList<>());

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFileName)))
        {
            String line;
            int chunkNumber = 0;
            List<String> lines = new LinkedList<>();
            List<Future<Boolean>> results = new LinkedList<>();

            while ((line = reader.readLine()) != null)
            {
                lines.add(line);

                if (lines.size() == maxLinesInMemory)
                {
                    List<String> _lines = new LinkedList<>(lines);
                    String chunkFileName = String.format(CHUNKIFY_FILE_NAME_PATTERN, chunkNumber++);
                    Future<Boolean> success = executorService.submit(() ->
                    {
                        try
                        {
                            sortChunk(_lines, chunkFileName);
                            chunkFiles.add(chunkFileName);
                            return true;
                        }
                        catch (Exception exception)
                        {
                            logger.error("sortChunk() failed", exception);
                            return false;
                        }
                    });

                    results.add(success);
                    lines.clear();
                }
            }

            if (!lines.isEmpty())
            {
                String chunkFileName = String.format(CHUNKIFY_FILE_NAME_PATTERN, chunkNumber);
                sortChunk(lines, chunkFileName);
                chunkFiles.add(chunkFileName);
            }

            for (Future<Boolean> result : results)
            {
                if (!result.get())
                    throw new Exception("chunkify() failed");
            }
        }
        catch (Exception exception)
        {
            logger.error("chunkify() failed");
            throw exception;
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
        catch (Exception exception)
        {
            logger.error("sortChunk() failed");
            throw exception;
        }
    }

    /**
     * Recursively merges smaller sorted chunks into bigger chunks until the whole file is sorted
     *
     * @param chunkFiles       the chunk files to be sorted
     * @param outputFileName   the name of the output file in which the sorted data will be written to
     * @param maxLinesInMemory maximum number of lines that can be held in memory at any given time
     * @param level            recursion level argument, used to define the file name
     * @throws Exception in case the merge operation fails
     */
    private static void mergeSortedChunks(List<String> chunkFiles, String outputFileName, int maxLinesInMemory, int level) throws Exception
    {
        int chunkNumber = 0;

        List<String> mergedChunkFiles = Collections.synchronizedList(new LinkedList<>());
        List<String> _chunkFiles = new LinkedList<>();

        if (chunkFiles.size() == 1) //termination point
        {
            renameFile(chunkFiles.get(0), outputFileName);
            return;
        }

        try
        {
            List<Future<Boolean>> results = new LinkedList<>();

            for (String chunkFile : chunkFiles)
            {
                _chunkFiles.add(chunkFile);

                if (_chunkFiles.size() == maxLinesInMemory)
                {
                    List<String> tempFiles = new LinkedList<>(_chunkFiles);
                    String mergedFileName = String.format(MERGE_FILE_NAME_PATTERN, level, chunkNumber++);
                    Future<Boolean> success = executorService.submit(() ->
                    {
                        try
                        {
                            doMergeSortedChunks(tempFiles, mergedFileName);
                            mergedChunkFiles.add(mergedFileName);
                            return true;
                        }
                        catch (Exception exception)
                        {
                            logger.error("mergeSortedChunks() failed", exception);
                            return false;
                        }
                    });

                    results.add(success);
                    _chunkFiles.clear();
                }
            }

            if (!_chunkFiles.isEmpty())
            {
                String mergedFileName = String.format(MERGE_FILE_NAME_PATTERN, level, chunkNumber);
                doMergeSortedChunks(_chunkFiles, mergedFileName);
                mergedChunkFiles.add(mergedFileName);
            }

            for (Future<Boolean> result : results)
            {
                if (!result.get())
                    throw new Exception("mergeSortedChunks() failed");
            }

            if (!mergedChunkFiles.isEmpty())
                mergeSortedChunks(mergedChunkFiles, outputFileName, maxLinesInMemory, level + 1);
        }
        catch (Exception exception)
        {
            logger.error("mergeSortedChunks() failed");
            throw exception;
        }
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
            finally
            {
                deleteFiles(chunkFiles, readers);
            }
        }
        catch (Exception exception)
        {
            logger.error("doMergeSortedChunks() failed");
            throw exception;
        }
    }

    private static void renameFile(String oldFileName, String newFileName) throws Exception
    {
        Path source = Paths.get(oldFileName);
        Files.move(source, source.resolveSibling(newFileName));
    }

    private static void deleteFiles(List<String> chunkFiles, Map<String, BufferedReader> readers)
    {
        for (String file : chunkFiles)
        {
            try
            {
                readers.get(file).close();
                Files.delete(Path.of(file));
            }
            catch (Exception exception)
            {
                logger.error("Error deleting file {}", file, exception);
            }
        }
    }
}