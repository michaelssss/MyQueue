package com.michaelssss.queue.serialization;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by michaelssss on 2017/9/16.
 */
public class SplitFileStorage implements Storage {

    private Path path;

    public static SplitFileStorage getInstance(String topic) {
        return new SplitFileStorage(topic);
    }

    private SplitFileStorage(String topic) {
        preWork(topic);
        path = Paths.get(this.getClass().getResource("/").getPath().substring(1), topic);
    }

    private SplitFileStorage() {
    }

    public static Collection<String> loadIndex(String topic) {
        Collection<String> uuids = new ArrayList<>(128);
        try {
            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(SplitFileStorage.class.getResource("/").getPath().substring(1), topic));
            Iterator<Path> iterator = directoryStream.iterator();
            while (iterator.hasNext()) {
                Path path1 = iterator.next();
                uuids.add(path1.relativize(Paths.get(SplitFileStorage.class.getResource("/").getPath().substring(1), topic)).toString());
            }
        } catch (Exception e) {

        }
        return uuids;
    }

    @Override
    public void save(String uuid, byte[] object) throws IOException {
        Files.write(path.resolve(uuid), object);
    }

    @Override
    public void delete(String uuid) throws IOException {
        Files.deleteIfExists(path.resolve(uuid));
    }

    @Override
    public void update(String uuid, byte[] object) throws IOException {
        delete(uuid);
        save(uuid, object);
    }

    @Override
    public byte[] get(String uuid) throws IOException {
        return Files.readAllBytes(path.resolve(uuid));
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

    }

    private static void preWork(String topic) {
        try {
            String path = FileStorageMessage.class.getResource("/").getPath().substring(1);
            if (Files.notExists(Paths.get(path, topic)))
                Files.createDirectory(Paths.get(path, topic));
        } catch (Exception e) {
            System.err.printf("create dir failed" + e.getLocalizedMessage());
        }
    }

}
