package com.michaelssss.queue;

import com.michaelssss.queue.serialization.CompressFileStorage;
import com.michaelssss.queue.serialization.Message;
import com.michaelssss.queue.serialization.Storage;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by michaelssss on 2017/9/16.
 */
public class MyQueue<T extends Serializable> extends ConcurrentLinkedQueue<Message> implements Queue<T> {
    private String topic;
    private Storage storage;

    public MyQueue(String topic) {
        storage = CompressFileStorage.getInstance(topic);
        this.topic = topic;

    }

    private void loadStorage() {
        Collection<String> uuids = storage.loadIndex();
        for (String uuid : uuids) {
            Message message = Message.create(storage, uuid, (String) topic);
            try {
                message.load();
                if (!message.isCosumed()) {
                    offer(message);
                }
            } catch (Exception e) {
                System.err.println(e.getLocalizedMessage());
                continue;
            }
        }
    }

    public void enQueue(T o) {
        Message message = Message.create(storage, topic, o);
        message.commit();
        offer(message);
        System.out.println(message);
    }

    public T deQueue() {
        Message message = this.poll();
        if (null == message) {
            loadStorage();
        }
        message = this.poll();
        message.cosume();
        System.out.println(message);
        return (T) message.getMessage();
    }
}
