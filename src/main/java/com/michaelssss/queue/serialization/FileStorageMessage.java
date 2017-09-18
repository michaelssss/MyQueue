package com.michaelssss.queue.serialization;

import java.io.*;

/**
 * Created by michaelssss on 2017/9/16.
 */
public class FileStorageMessage extends Message {
    protected transient Storage storage;

    protected FileStorageMessage(Storage storage, String topic, Serializable message) {
        super(topic, message);
        this.storage = storage;
    }

    protected FileStorageMessage(Storage storage, String uuid, String topic) {
        this.uuid = uuid;
        this.topic = topic;
        this.storage = storage;
    }


    @Override
    public void update() {
        try {
            storage.update(this.uuid, getSerializationByte(this));
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
            storage.save(uuid, getSerializationByte(this));
        } catch (IOException e) {
            System.err.printf("save Object failed because" + e.getMessage());
        }
    }

    @Override
    public void load() throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(storage.get(uuid));
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
            storage.delete(uuid);
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
