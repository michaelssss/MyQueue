package com.michaelssss;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by michaelssss on 2017/9/16.
 */
public class IOUtils {
    private static final int EOF = -1;

    public static void copy(InputStream in, OutputStream os) {
        try {
            byte[] buffer = new byte[1024];
            int offset;
            while ((offset = in.read(buffer)) != EOF) {
                os.write(buffer, 0, offset);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
