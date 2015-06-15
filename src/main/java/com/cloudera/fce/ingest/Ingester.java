package com.cloudera.fce.ingest;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
        String idleTimeoutInSeconds = args[6];

        Properties properties = new Properties();
        properties.put("zookeeper.connect", zookeepers);
        properties.put("group.id", groupId);
        properties.put("ingester.topic", topic);
        properties.put("ingester.schemafile", schemaFile);
        properties.put("ingester.path", path);
        properties.put("ingester.blocksize", blocksize);
        properties.put("ingester.idleTimeoutInSeconds", idleTimeoutInSeconds);

        System.out.println(properties);

        Ingester ingester = new Ingester(properties);
        ingester.run();
    }

    public Ingester(Properties properties) throws Exception {

        properties.put("auto.offset.reset", "smallest");
        properties.put("auto.commit.enable", "false");

        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumer = Consumer.createJavaConsumerConnector(consumerConfig);

        schema = new Schema.Parser().parse(new File((String) properties.get("ingester.schemafile")));

        topic = (String) properties.get("ingester.topic");

        Path path = new Path((String) properties.get("ingester.path"));

        int blockSize = Integer.parseInt(properties.get("ingester.blocksize").toString());
        int pageSize = 1 * 1000 * 1000;
        long timeoutInMillis = Integer.parseInt(properties.get("ingester.idleTimeoutInSeconds").toString()) * 1000;

        writer = new RollingParquetWriter(path, schema, blockSize, pageSize, timeoutInMillis) {
            @Override
            public void onRoll() {
                consumer.commitOffsets();
                System.out.println("Rolling. Consumed " + count + " messages.");
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

    public void run() {
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