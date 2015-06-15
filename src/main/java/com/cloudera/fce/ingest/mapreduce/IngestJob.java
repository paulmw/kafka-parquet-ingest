package com.cloudera.fce.ingest.mapreduce;

import kafka.utils.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IngestJob implements Tool {

    private Configuration conf;

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    public int run(String[] args) throws Exception {

        conf.set("zookeeper.connect", args[0]);
        conf.set("group.id", args[1]);
        conf.set("ingester.schemafile", args[2]);
        conf.set("ingester.topic", args[3]);
        conf.set("ingester.path", args[4]);
        conf.set("ingester.blocksize", args[5]);
        conf.set("ingester.idleTimeoutInSeconds", args[6]);
        conf.set("ingester.tasks", args[7]);

        if(args[8].equals("true")) {
            ZkUtils.maybeDeletePath(conf.get("zookeeper.connect") + "", "/consumers/" + conf.get("group.id"));
        }

        conf.set("mapreduce.map.java.opts", "-Xmx8G");
        conf.set("mapreduce.map.memory.mb", "7500   ");

        Job job = Job.getInstance(conf, "Kafka Ingester");

        job.setMapperClass(IngestMapper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(IngestInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.setJarByClass(IngestJob.class);

        job.setNumReduceTasks(0);

        job.submit();
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        IngestJob ingestJob = new IngestJob();
        ToolRunner.run(ingestJob, args);
    }

}
