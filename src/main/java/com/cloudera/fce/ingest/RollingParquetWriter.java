package com.cloudera.fce.ingest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import parquet.avro.AvroSchemaConverter;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.lang.reflect.Field;
import java.util.UUID;

public abstract class RollingParquetWriter {

    private int blockSize;
    private int pageSize;
    private long timeoutInMillis;

    private Schema schema;
    private Path path;
    private ParquetWriter writer;

    private Path filePath;

    private HdfsDataOutputStream out;
    private long lastPosition = -1;

    private long lastWriteTime = -1;

    private Watchdog watchdog;
    private Thread watchdogThread;

    public RollingParquetWriter(Path path, Schema schema, int blockSize, int pageSize, long timeoutInMillis) {
        this.path = path;
        this.schema = schema;
        this.blockSize = blockSize;
        this.pageSize = pageSize;
        this.timeoutInMillis = timeoutInMillis;
    }

    public abstract void onRoll();

    private static class Watchdog implements Runnable {

        private long timeoutInMillis;
        private RollingParquetWriter writer;
        private boolean stopped;

        public Watchdog(RollingParquetWriter writer, long timeoutInMillis) {
            this.writer = writer;
            this.timeoutInMillis = timeoutInMillis;
        }

        @Override
        public void run() {
            while(!stopped) {
                try {
                    Thread.sleep(timeoutInMillis);
                } catch (InterruptedException e) {

                }
                long lastWriteTime = writer.getLastWriteTime();
                long currentTime = System.currentTimeMillis();
                if(lastWriteTime != -1 && currentTime - lastWriteTime > timeoutInMillis) {
                    try {
                        writer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        }

        public void stop() {
            stopped = true;
        }
    }

    private void init() throws Exception {
        if(writer == null) {
            MessageType parquetSchema = new AvroSchemaConverter().convert(schema);
            AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, schema);
            filePath = new Path(path.toString() + "/" + UUID.randomUUID().toString() + ".parquet.tmp");
            System.out.println("Writing to " + filePath);
            writer = new ParquetWriter(filePath, writeSupport, CompressionCodecName.SNAPPY, blockSize, pageSize, true, true);

            out = getHdfsDataOutputStream(writer);

            watchdog = new Watchdog(this, timeoutInMillis);
            watchdogThread = new Thread(watchdog);
            watchdogThread.setName("watchdog");
            watchdogThread.start();
        }
    }

    public void write(GenericRecord record) throws Exception {
        init();
        if(!record.getSchema().equals(schema)) {
            throw new IllegalArgumentException();
        }
        writer.write(record);
        lastWriteTime = System.currentTimeMillis();
        long currentPosition = out.getPos();
        if(currentPosition != lastPosition && lastPosition != -1) {
            close();
            return;
        }
        lastPosition = currentPosition;
    }

    private long getLastWriteTime() {
        return lastWriteTime;
    }

    public void close() throws Exception {
        writer.close();
        System.out.println("Closing " + filePath);
        writer = null;
        out = null;
        lastPosition = -1;
        lastWriteTime = -1;
        onRoll();
        watchdog.stop();
        watchdogThread.interrupt();
    }

    private HdfsDataOutputStream getHdfsDataOutputStream(ParquetWriter writer) throws Exception {
        Field f = writer.getClass().getDeclaredField("writer");
        f.setAccessible(true);
        Object internalParquetRecordWriter = f.get(writer);
        f = internalParquetRecordWriter.getClass().getDeclaredField("w");
        f.setAccessible(true);
        Object parquetFileWriter = f.get(internalParquetRecordWriter);
        f = parquetFileWriter.getClass().getDeclaredField("out");
        f.setAccessible(true);
        return (HdfsDataOutputStream) f.get(parquetFileWriter);
    }

}
