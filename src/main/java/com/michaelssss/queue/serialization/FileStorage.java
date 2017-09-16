package com.michaelssss.queue.serialization;

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
public class FileStorage {
    private static final FileStorage instance = new FileStorage();

    public static FileStorage getInstance() {
        return instance;
    }

    private FileStorage() {
    }

    public Collection<String> loadAllMessageInTopic(String topic) {
        Collection<String> uuids = new ArrayList<>(128);
        try {
            String path = this.getClass().getResource("/").getPath().substring(1);
            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(path, topic));
            Iterator<Path> iterator = directoryStream.iterator();
            while (iterator.hasNext()) {
                Path path1 = iterator.next();
                uuids.add(path1.relativize(Paths.get(path, topic)).toString());
            }
        } catch (Exception e) {

        }
        return uuids;
    }
}
