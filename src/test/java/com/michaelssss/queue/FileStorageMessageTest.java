package com.michaelssss.queue;

import com.michaelssss.queue.serialization.FileStorageMessage;
import com.michaelssss.queue.serialization.Message;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

/**
 * Created by michaelssss on 2017/9/16.
 */
public class FileStorageMessageTest {
    private Message message;

    @Before
    public void before() {
        message = new FileStorageMessage("talk", (Serializable) "hehehehe");
    }

    @Test
    public void testCommit() {
        System.out.printf(message.toString());
        message.commit();
    }

    @Test
    public void testUpdate() {
        System.out.printf(message.toString());
        message.commit();
        message.cosume();
        message.update();
        System.out.printf(message.toString());
    }

    @Test
    public void testReload() {
        message = new FileStorageMessage("1505539129268", "talk");
        System.out.printf(message.toString()+"\n");
        try {
            message.load();
            System.out.printf(message.toString()+"\n");
            message.cosume();
            System.out.printf(message.toString()+"\n");
            message = new FileStorageMessage("1505539129268", "talk");
            System.out.printf(message.toString()+"\n");
            message.load();
            System.out.printf(message.toString()+"\n");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
