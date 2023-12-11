package com.v.sort;

public class LineFileDescriptor implements Comparable<LineFileDescriptor>
{
    private String line;
    private String fileName;

    public LineFileDescriptor(String line, String fileName)
    {
        this.line = line;
        this.fileName = fileName;
    }

    public String getLine()
    {
        return line;
    }

    public String getFileName()
    {
        return fileName;
    }

    @Override
    public int compareTo(LineFileDescriptor descriptor)
    {
        return Integer.parseInt(this.getLine()) - Integer.parseInt(descriptor.getLine());
    }
}
