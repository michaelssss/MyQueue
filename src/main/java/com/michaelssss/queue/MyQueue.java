package com.michaelssss.queue;

import com.michaelssss.queue.serialization.FileStorage;
import com.michaelssss.queue.serialization.FileStorageMessage;
import com.michaelssss.queue.serialization.Message;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by michaelssss on 2017/9/16.
 */
public class MyQueue<T extends Serializable> extends ConcurrentLinkedQueue<Message> implements Queue<T> {
    private String topic;
    private Collection<String> uuids;

    public MyQueue(String topic) {
        this.topic = topic;
        uuids = FileStorage.getInstance().loadAllMessageInTopic(topic);
    }

    public void enQueue(T o) {
        Message message = new FileStorageMessage(topic, o);
        message.commit();
        offer(message);
        System.out.println(message);
    }

    public T deQueue() {
        try {
            Message message = this.element();
            message.cosume();
            System.out.println(message);
            return (T) message.getMessage();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
