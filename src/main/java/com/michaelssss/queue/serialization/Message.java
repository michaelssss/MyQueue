package com.michaelssss.queue.serialization;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

/**
 * Created by michaelssss on 2017/9/16.
 */
public abstract class Message implements Serializable {

    private static final long serialVersionUID = -860906539865407283L;
    protected String uuid;
    protected String topic;
    protected Date produceTime;
    protected Date cosumingTime;
    protected Serializable message;


    protected Message() {
    }

    public static Message create(Storage storage, String topic, Serializable message) {
        return new FileStorageMessage(storage, topic, message);
    }

    public static Message create(Storage storage, String uuid, String topic) {
        return new FileStorageMessage(storage, uuid, topic);
    }

    protected Message(String topic, Serializable message) {
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
