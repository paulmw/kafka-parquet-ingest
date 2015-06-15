package com.cloudera.fce.ingest.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IngestSplit extends InputSplit implements Writable {

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    public IngestSplit() {
        super();
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }
}