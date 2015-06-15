package com.cloudera.fce.ingest;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.ConsumerRebalanceListener;
import kafka.javaapi.consumer.ZookeeperConsumerConnector;
import kafka.utils.ZkUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.InputStream;
import java.util.*;

public class Ingester {

    private String topic = "netflow";
    private ConsumerConnector consumer;
    private Schema schema;
    private RollingParquetWriter writer;

    public static void main(String[] args) throws Exception {
        String zookeepers = args[0];
        String groupId = args[1];
        String schemaFile = args[2];
        String topic = args[3];
        String path = args[4];
        String blocksize = args[5];
        String pagesize = args[6];
        String idleTimeoutInSeconds = args[7];

        Properties properties = new Properties();
        properties.put("zookeeper.connect", zookeepers);
        properties.put("group.id", groupId);
        properties.put("ingester.topic", topic);
        properties.put("ingester.schemafile", schemaFile);
        properties.put("ingester.path", path);
        properties.put("ingester.blocksize", blocksize);
        properties.put("ingester.pagesize", pagesize);
        properties.put("ingester.idleTimeoutInSeconds", idleTimeoutInSeconds);

        System.out.println(properties);

        if(args[8].equals("true")) {
            ZkUtils.maybeDeletePath(properties.get("zookeeper.connect") + "", "/consumers/" + properties.get("group.id"));
        }

        Ingester ingester = new Ingester(properties);
        ingester.run(null);
    }

    public Ingester(Properties properties) throws Exception {

        properties.put("auto.offset.reset", "smallest");
        properties.put("auto.commit.enable", "false");

        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumer = Consumer.createJavaConsumerConnector(consumerConfig);

        ZookeeperConsumerConnector zkcc = (ZookeeperConsumerConnector) consumer;
        zkcc.setConsumerRebalanceListener(new ConsumerRebalanceListener() {
            @Override
            public void beforeReleasingPartitions(Map<String, Set<Integer>> partitionOwnership) {
                try {
                    System.out.println("Rebalancing - closing down.");
                    writer.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        });

        FileSystem fs = FileSystem.get(new Configuration());
        InputStream is = fs.open(new Path((String) properties.get("ingester.schemafile")));

        schema = new Schema.Parser().parse(is);
        is.close();

        topic = (String) properties.get("ingester.topic");

        Path path = new Path((String) properties.get("ingester.path"));

        int blockSize = Integer.parseInt(properties.get("ingester.blocksize").toString());
        int pageSize = Integer.parseInt(properties.get("ingester.pagesize").toString());
        long timeoutInMillis = Integer.parseInt(properties.get("ingester.idleTimeoutInSeconds").toString()) * 1000;

        writer = new RollingParquetWriter(fs, path, schema, blockSize, pageSize, timeoutInMillis) {
            @Override
            public void onRoll() {
                consumer.commitOffsets();
                System.out.println("Rolling. Consumed " + count + " messages.");
                if(context != null) {
                    context.setStatus("Rolling. Consumed " + count + " messages.");
                }
            }
        };

    }

    public GenericRecord deserialise(byte[] b) throws Exception {
        GenericRecord record = null;
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        BinaryDecoder d = null;
        d = DecoderFactory.get().binaryDecoder(b, d);
        record = datumReader.read(record, d);
        return record;
    }

    private int count;

    private Context context;

    public void run(Context context) {
        this.context = context;

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);

        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        count = 0;
        while (it.hasNext()) {
            byte[] message = it.next().message();
            try {
                GenericRecord record = deserialise(message);
                writer.write(record);
                count++;
                if (count % 100000 == 0) {
                    System.out.println("Read " + count + " events into Parquet.");
                    if(context != null) {
                        context.setStatus("Read " + count + " events into Parquet.");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}