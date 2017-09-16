package com.michaelssss;

import com.michaelssss.queue.MyQueue;
import com.michaelssss.queue.Queue;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * Created by michaelssss on 2017/9/16.
 */
public class Starter {
    private static final Queue<String> queue = new MyQueue<>("testTopic");

    public static void main(String[] args) {
        try {
            HttpServer httpServer = HttpServer.create(new InetSocketAddress(8080), 0);
            httpServer.createContext("/enqueue", new MyHandler());
            httpServer.setExecutor(null); // creates a default executor
            httpServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            InputStream in = t.getRequestBody();
            ByteArrayOutputStream os1 = new ByteArrayOutputStream();
            IOUtils.copy(in, os1);
            queue.enQueue(os1.toString());
            t.sendResponseHeaders(200, os1.toByteArray().length);
            OutputStream os = t.getResponseBody();
            os.write(queue.deQueue().getBytes());
            os.close();
        }
    }
}
