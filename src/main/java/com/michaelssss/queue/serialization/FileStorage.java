package com.michaelssss.queue.serialization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by michaelssss on 2017/9/16.
 */
public class FileStorage {

    private Path path;

    public static FileStorage getInstace(String topic) {
        return new FileStorage(topic);
    }

    private FileStorage(String topic) {
        preWork(topic);
        path = Paths.get(this.getClass().getResource("/").getPath().substring(1), topic);
    }

    private FileStorage() {
    }

    public void save(String uuid, byte[] object) throws IOException {
        Files.write(path.resolve(uuid), object);
    }

    public void delete(String uuid) throws IOException {
        Files.deleteIfExists(path.resolve(uuid));
    }

    public void update(String uuid, byte[] object) throws IOException {
        delete(uuid);
        save(uuid, object);
    }

    public byte[] get(String uuid) throws IOException {
        return Files.readAllBytes(path.resolve(uuid));
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
