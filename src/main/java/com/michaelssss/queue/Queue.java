package com.michaelssss.queue;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by michaelssss on 2017/9/16.
 */
public interface Queue<T> extends Serializable {
    void enQueue(T t);

    T deQueue();
}
