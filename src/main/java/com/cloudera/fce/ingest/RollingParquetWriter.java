package com.cloudera.fce.ingest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import parquet.avro.AvroSchemaConverter;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class RollingParquetWriter {

    private int blockSize;
    private int pageSize;
    private long timeoutInMillis;

    private Schema schema;
    private Path path;
    private ParquetWriter writer;

    private Path filePath;
    private FileSystem fs;

    private HdfsDataOutputStream out;
    private long lastPosition = -1;

    private long lastWriteTime = -1;

    private Watchdog watchdog;
    private Thread watchdogThread;

    public RollingParquetWriter(FileSystem fs, Path path, Schema schema, int blockSize, int pageSize, long timeoutInMillis) {
        this.path = path;
        this.schema = schema;
        this.blockSize = blockSize;
        this.pageSize = pageSize;
        this.timeoutInMillis = timeoutInMillis;
        this.fs = fs;
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
            while (!stopped) {
                try {
                    Thread.sleep(timeoutInMillis);
                } catch (InterruptedException e) {

                }
                long lastWriteTime = writer.getLastWriteTime();
                long currentTime = System.currentTimeMillis();
                if (lastWriteTime != -1 && currentTime - lastWriteTime > timeoutInMillis) {
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
        if (writer == null) {
            MessageType parquetSchema = new AvroSchemaConverter().convert(schema);
            AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, schema);
            filePath = new Path(path.toString() + "/" + UUID.randomUUID().toString() + ".parquet");
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
        if (!record.getSchema().equals(schema)) {
            throw new IllegalArgumentException();
        }
        writer.write(record);
        lastWriteTime = System.currentTimeMillis();
        long currentPosition = out.getPos();
        if (currentPosition != lastPosition && lastPosition != -1) {
            close();
            return;
        }
        lastPosition = currentPosition;
    }

    private long getLastWriteTime() {
        return lastWriteTime;
    }

    public void close() throws Exception {
        if(writer != null) {
            writer.close();
            System.out.println("Closing " + filePath);
            fs.rename(path, new Path(path.toString().replace(".tmp", "")));
            writer = null;
            out = null;
            lastPosition = -1;
            lastWriteTime = -1;
            onRoll();
            watchdog.stop();
            watchdogThread.interrupt();
        }
    }

    private <T> void searchForInstanceOfType(Class<T> t, Object o, List<T> candidates) throws Exception {
        List<Object> previous = new ArrayList<Object>();
        searchForInstanceOfType(t, o, candidates, previous);
    }

    private <T> void searchForInstanceOfType(Class<T> t, Object o, List<T> candidates, List<Object> previous) throws Exception {
        if (previous.contains(o)) {
            return;
        }
        previous.add(o);
        if (o != null) {
            Field[] fields = o.getClass().getDeclaredFields();
            for (Field f : fields) {
                boolean accessible = f.isAccessible();
                f.setAccessible(true);
                Class c = f.getType();
                Class d = f.get(o) != null ? f.get(o).getClass() : null;
                if (!c.isPrimitive()) {
                    Package p = c.getPackage();
                    if (p != null && !p.getName().startsWith("java")) {
                        if (c.equals(t) || (d != null && d.equals(t))) {
                            candidates.add((T) f.get(o));
                        }
                        searchForInstanceOfType(t, f.get(o), candidates, previous);
                    }
                }
                f.setAccessible(accessible);
            }
        }
    }

    private HdfsDataOutputStream getHdfsDataOutputStream(ParquetWriter writer) throws Exception {
        List<HdfsDataOutputStream> candidates = new ArrayList<HdfsDataOutputStream>();
        searchForInstanceOfType(HdfsDataOutputStream.class, writer, candidates);
        if (candidates.size() == 1) {
            return candidates.get(0);
        } else {
            throw new IllegalStateException("Unable to automagically find the wumpus...");
        }
    }

}
