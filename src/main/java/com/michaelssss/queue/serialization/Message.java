package com.michaelssss.queue.serialization;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

/**
 * Created by michaelssss on 2017/9/16.
 */
public abstract class Message implements Serializable {
    protected String uuid;
    protected String topic;
    protected Date produceTime;
    protected Date cosumingTime;
    protected Serializable message;

    public Message() {
    }

    public static Collection<String> loadAllMessageInTopic(String topic) {
        Collection<String> uuids = new ArrayList<>(128);
        try {
            String path = Message.class.getResource("/").getPath().substring(1);
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

    public Message(String topic, Serializable message) {
        this.uuid = Long.toString(System.currentTimeMillis());
        this.message = message;
        this.topic = topic;
        produceTime = new Date();
    }

    public void cosume() {
        this.cosumingTime = new Date();
        update();
    }

    public boolean isCosumed() {
        return null != cosumingTime;
    }

    public abstract Object getMessage();

    public abstract void update();

    public abstract void commit();

    public abstract void load() throws IOException, ClassNotFoundException;

    public abstract void delete();
}
