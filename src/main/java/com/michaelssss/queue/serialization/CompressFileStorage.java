package com.michaelssss.queue.serialization;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author michaelssss
 * @since 2017/9/18
 */
public class CompressFileStorage implements Storage {
    private RandomAccessFile randomAccessFile;
    private int lastFileIndex;
    private ReentrantLock lock;
    private Map<String, myFile> filename_offset = new ConcurrentHashMap<>();
    private Path path;

    public static Storage getInstance(String topic) {
        return new CompressFileStorage(topic);
    }

    private CompressFileStorage() {
    }

    public static Collection<String> loadIndex(String topic) {
        Collection<String> uuids = new ArrayList<>(128);
        try {
            Path path = Paths.get(CompressFileStorage.class.getResource("/").getPath().substring(1), topic);
            byte[] bytes = Files.readAllBytes(path.resolve("index"));
            if (bytes.length != 0) {
                ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
                Index index = (Index) objectInputStream.readObject();
                if (null != index) {
                    uuids = index.index.keySet();
                }
            }
        } catch (IOException | ClassNotFoundException e) {

        }
        return uuids;
    }

    private CompressFileStorage(String topic) {
        this.lock = new ReentrantLock();
        path = Paths.get(this.getClass().getResource("/").getPath().substring(1), topic);
        preWork(path);
        try {
            //initial index
            //open data file with random access
            randomAccessFile = new RandomAccessFile(path.resolve("data").toString(), "rw");
            randomAccessFile.setLength(1024 * 1024);
            byte[] bytes = Files.readAllBytes(path.resolve("index"));
            if (bytes.length != 0) {
                ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
                Index index = (Index) objectInputStream.readObject();
                if (null != index) {
                    lastFileIndex = index.lastFileIndex;
                    filename_offset = index.index;
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println(e.getLocalizedMessage());
        }
    }

    private static void preWork(Path filepath) {
        try {

            if (Files.notExists(filepath))
                Files.createDirectory(filepath);
            if (Files.notExists(filepath.resolve("index")))
                Files.createFile(filepath.resolve("index"));
            if (Files.notExists(filepath.resolve("data")))
                Files.createFile(filepath.resolve("data"));

        } catch (IOException e) {
            System.err.println(e.getLocalizedMessage());
        }
    }

    @Override
    public void save(String uuid, byte[] object) throws IOException {
        lock.lock();
        randomAccessFile.seek(lastFileIndex);
        randomAccessFile.write(object, 0, object.length);
        randomAccessFile.getFD().sync();
        filename_offset.put(uuid, new myFile(lastFileIndex, object.length));
        lastFileIndex = lastFileIndex + object.length;
        saveIndex();
        lock.unlock();
    }

    @Override
    public void delete(String uuid) throws IOException {
        lock.lock();
        filename_offset.remove(uuid);
        saveIndex();
        lock.unlock();
    }

    @Override
    public void update(String uuid, byte[] object) throws IOException {
        lock.lock();
        delete(uuid);
        save(uuid, object);
        lock.unlock();
    }

    @Override
    public byte[] get(String uuid) throws IOException {
        lock.lock();
        myFile myFile = filename_offset.get(uuid);
        byte[] file = new byte[myFile.offset];
        randomAccessFile.seek(myFile.start);
        randomAccessFile.read(file, 0, myFile.offset);
        lock.unlock();
        return file;
    }

    private void saveIndex() throws IOException {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(bs);
        objectOutputStream.writeObject(new Index(filename_offset, lastFileIndex));
        objectOutputStream.flush();
        objectOutputStream.close();
        Files.deleteIfExists(path.resolve("index"));
        Files.createFile(path.resolve("index"));
        Files.write(path.resolve("index"), bs.toByteArray());
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     * <p>
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        lock.lock();
        saveIndex();
        randomAccessFile.close();
        lock.unlock();
    }

    static class Index implements Serializable {
        public Index(Map index, int lastFileIndex) {
            this.index = index;
            this.lastFileIndex = lastFileIndex;
        }

        Map index;
        int lastFileIndex;

        @Override
        public String toString() {
            return "Index{" +
                    "index=" + index +
                    ", lastFileIndex=" + lastFileIndex +
                    '}';
        }
    }

    static class myFile implements Serializable {

        private static final long serialVersionUID = -3553034060783025915L;

        public myFile(int start, int offset) {
            this.start = start;
            this.offset = offset;
        }

        int start;
        int offset;

        @Override
        public String toString() {
            return "myFile{" +
                    "start=" + start +
                    ", offset=" + offset +
                    '}';
        }
    }
}
