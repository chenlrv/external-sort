package com.v.sort;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExternalSortTests
{
    private static final String TEST1_FILE_NAME = "./src/test/resources/test1.csv";
    private static final String TEST2_FILE_NAME = "./src/test/resources/test2.csv";
    private static final String TEST3_FILE_NAME = "./src/test/resources/test3.csv";
    private static final String TEST4_FILE_NAME = "./src/test/resources/test4.csv";
    private static final String OUTPUT_FILE_NAME = "./src/test/resources/test_output.csv";

    private static final Logger logger = LogManager.getLogger(ExternalSortTests.class);

    @Test
    public void testSort() throws Exception
    {
        ExternalSort.main(new String[]{TEST1_FILE_NAME, OUTPUT_FILE_NAME, "3"});

        List<String> internalSortLines = getLines(TEST1_FILE_NAME, true);
        List<String> externalSortLines = getLines(OUTPUT_FILE_NAME, false);

        assertEquals(internalSortLines, externalSortLines);
    }

    @Test
    public void testSort2() throws Exception
    {
        ExternalSort.main(new String[]{TEST2_FILE_NAME, OUTPUT_FILE_NAME, "5"});

        List<String> internalSortLines = getLines(TEST2_FILE_NAME, true);
        List<String> externalSortLines = getLines(OUTPUT_FILE_NAME, false);

        assertEquals(internalSortLines, externalSortLines);
    }

    @Test
    public void testSort3() throws Exception
    {
        ExternalSort.main(new String[]{TEST3_FILE_NAME, OUTPUT_FILE_NAME, "20"});

        List<String> internalSortLines = getLines(TEST3_FILE_NAME, true);
        List<String> externalSortLines = getLines(OUTPUT_FILE_NAME, false);

        assertEquals(internalSortLines, externalSortLines);
    }

    @Test
    public void testSort4() throws Exception
    {
        ExternalSort.main(new String[]{TEST4_FILE_NAME, OUTPUT_FILE_NAME, "12"});

        List<String> internalSortLines = getLines(TEST4_FILE_NAME, true);
        List<String> externalSortLines = getLines(OUTPUT_FILE_NAME, false);

        assertEquals(internalSortLines, externalSortLines);
    }

    @AfterEach
    public void after()
    {
        try
        {
            Files.deleteIfExists(Path.of(OUTPUT_FILE_NAME));
        }
        catch (Exception exception)
        {
            logger.error("Error in after(): ", exception);
        }
    }

    private static List<String> getLines(String fileName, boolean sort) throws Exception
    {
        List<String> result = new LinkedList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName)))
        {
            String line;

            while ((line = reader.readLine()) != null)
                result.add(line);
        }

        if (sort)
            result.sort(Comparator.comparingInt(Integer::parseInt));

        return result;
    }
}
