package com.michaelssss.queue.serialization;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author michaelssss
 * @since 2017/9/18
 */
public interface Storage extends Closeable {

    void save(String uuid, byte[] object) throws IOException;

    void delete(String uuid) throws IOException;

    void update(String uuid, byte[] object) throws IOException;

    byte[] get(String uuid) throws IOException;

}
