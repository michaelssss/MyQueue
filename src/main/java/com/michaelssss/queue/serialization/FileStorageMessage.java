package com.michaelssss.queue.serialization;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by michaelssss on 2017/9/16.
 */
public class FileStorageMessage extends Message {
    public FileStorageMessage(String topic, Serializable message) {
        super(topic, message);
        preWork(topic);
    }

    public FileStorageMessage(String uuid, String topic) {
        this.uuid = uuid;
        this.topic = topic;
        preWork(topic);
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

    @Override
    public void update() {
        try {
            String path = this.getClass().getResource("/").getPath().substring(1);
            Files.delete(Paths.get(path, topic, uuid));
            commit();
        } catch (Exception e) {
            System.err.printf("update Object failed because" + e.getMessage());
        }
    }


    @Override
    public Object getMessage() throws IOException, ClassNotFoundException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(byteArrayOutputStream);
        o.writeObject(this.message);
        o.flush();
        o.close();
        ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        inputStream.close();
        return inputStream.readObject();
    }

    @Override
    public void commit() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream o = new ObjectOutputStream(byteArrayOutputStream);
            o.writeObject(this);
            o.flush();
            o.close();
            String path = this.getClass().getResource("/").getPath().substring(1);
            Files.write(Paths.get(path, topic, uuid), byteArrayOutputStream.toByteArray());
        } catch (Exception e) {
            System.err.printf("save Object failed because" + e.getMessage());
        }
    }

    @Override
    public void load() throws IOException, ClassNotFoundException {
        String path = this.getClass().getResource("/").getPath().substring(1);
        byte[] bytes = Files.readAllBytes(Paths.get(path, topic, uuid));
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        Object o = objectInputStream.readObject();
        if (o instanceof Message) {
            Message message = (Message) o;
            this.produceTime = message.produceTime;
            this.cosumingTime = message.cosumingTime;
            this.message = message.message;
        } else {
            throw new IOException(o.getClass().getSimpleName());
        }
    }

    @Override
    public void delete() {
        try {
            String path = this.getClass().getResource("/").getPath().substring(1);
            Files.deleteIfExists(Paths.get(path, topic, uuid));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "FileStorageMessage{" +
                "uuid='" + uuid + '\'' +
                ", topic='" + topic + '\'' +
                ", produceTime=" + produceTime +
                ", cosumingTime=" + cosumingTime +
                ", message=" + message +
                '}';
    }
}
