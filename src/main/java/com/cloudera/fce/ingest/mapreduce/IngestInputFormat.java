package com.cloudera.fce.ingest.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IngestInputFormat extends InputFormat<IntWritable, NullWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        Configuration conf = context.getConfiguration();
        int mappers = conf.getInt("ingester.tasks", 1);
        for (int i = 0; i < mappers; i++) {
            splits.add(new IngestSplit());
        }
        return splits;
    }


    @Override
    public RecordReader<IntWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        RecordReader recordReader = new IngestRecordReader();
        recordReader.initialize(split, context);
        return recordReader;
    }


}
