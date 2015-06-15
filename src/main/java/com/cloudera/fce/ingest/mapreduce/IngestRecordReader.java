package com.cloudera.fce.ingest.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class IngestRecordReader extends RecordReader<IntWritable, NullWritable> {

    private boolean generated;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        generated = false;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!generated) {
            generated = true;
        }
        return generated;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException,
            InterruptedException {
        return new IntWritable(0);
    }

    @Override
    public NullWritable getCurrentValue() throws IOException,
            InterruptedException {
        return NullWritable.get();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return generated ? 0 : 1;
    }

    @Override
    public void close() throws IOException {
        // NOP
    }

}
