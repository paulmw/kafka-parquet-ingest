package com.cloudera.fce.ingest.mapreduce;

import com.cloudera.fce.ingest.Ingester;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Properties;

public class IngestMapper extends Mapper<IntWritable, NullWritable, IntWritable, NullWritable> {

    private Configuration conf;
    private Properties properties;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        properties = new Properties();
        properties.put("zookeeper.connect", conf.get("zookeeper.connect"));
        properties.put("zookeeper.session.timeout.ms", conf.get("zookeeper.session.timeout.ms"));
        properties.put("group.id", conf.get("group.id"));
        properties.put("ingester.schemafile", conf.get("ingester.schemafile"));
        properties.put("ingester.topic", conf.get("ingester.topic"));
        properties.put("ingester.blocksize", conf.get("ingester.blocksize"));
        properties.put("ingester.pagesize", conf.get("ingester.pagesize"));
        properties.put("ingester.path", conf.get("ingester.path"));
        properties.put("ingester.idleTimeoutInSeconds", conf.get("ingester.idleTimeoutInSeconds"));
    }

    @Override
    protected void map(IntWritable key, NullWritable value, Mapper.Context context) throws IOException, InterruptedException {
        try {
            Ingester ingester = new Ingester(properties);
            ingester.run(context);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
