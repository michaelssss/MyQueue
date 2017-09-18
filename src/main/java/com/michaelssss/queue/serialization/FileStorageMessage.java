package com.michaelssss.queue.serialization;

import java.io.*;

/**
 * Created by michaelssss on 2017/9/16.
 */
public class FileStorageMessage extends Message {
    private transient FileStorage fileStorage;

    public FileStorageMessage(String topic, Serializable message) {
        super(topic, message);
        fileStorage = FileStorage.getInstace(topic);
    }

    public FileStorageMessage(String uuid, String topic) {
        this.uuid = uuid;
        this.topic = topic;
        fileStorage = FileStorage.getInstace(topic);
    }


    @Override
    public void update() {
        try {
            fileStorage.update(this.uuid, getSerializationByte(this));
        } catch (IOException e) {
            //assert save never failed
        }
    }


    @Override
    public Object getMessage() {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(getSerializationByte(this.message)));
            inputStream.close();
            return inputStream.readObject();
        } catch (Exception e) {
            System.err.println("copy Object failed because " + e.getLocalizedMessage());
        }
        return null;
    }

    @Override
    public void commit() {
        try {
            fileStorage.save(uuid, getSerializationByte(this));
        } catch (IOException e) {
            System.err.printf("save Object failed because" + e.getMessage());
        }
    }

    @Override
    public void load() throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(fileStorage.get(uuid));
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
            fileStorage.delete(uuid);
        } catch (IOException e) {
            //assert delete never failed
        }
    }

    private static byte[] getSerializationByte(Serializable serializable) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(byteArrayOutputStream);
        o.writeObject(serializable);
        o.flush();
        o.close();
        return byteArrayOutputStream.toByteArray();
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
