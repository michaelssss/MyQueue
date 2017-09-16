package com.michaelssss.queue.serialization;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

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

    public abstract Object getMessage() throws IOException, ClassNotFoundException;

    public abstract void update();

    public abstract void commit();

    public abstract void load() throws IOException, ClassNotFoundException;
}
