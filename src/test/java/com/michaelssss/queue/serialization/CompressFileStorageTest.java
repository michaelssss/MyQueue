package com.michaelssss.queue.serialization;

import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * @author michaelssss
 * @since 2017/9/18
 */
public class CompressFileStorageTest {
    private Storage storage;

    @Before
    public void save() {
        storage = CompressFileStorage.getInstance("testTopic");
    }

    @After
    public void after1() {
        try {
            storage.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void after() {
        try {
            //clean test file
            Files.delete(Paths.get(CompressFileStorage.class.getResource("/").getPath().substring(1), "testTopic", "index"));
            Files.delete(Paths.get(CompressFileStorage.class.getResource("/").getPath().substring(1), "testTopic", "data"));
            Files.delete(Paths.get(CompressFileStorage.class.getResource("/").getPath().substring(1), "testTopic"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSave() {
        try {
            storage.save("1", "hahahaha".getBytes());
            storage.close();
            showIndex();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getLocalizedMessage());
        }
    }

    private void showIndex() {
        try {
            Path path = Paths.get(CompressFileStorage.class.getResource("/").getPath().substring(1), "testTopic", "index");
            byte[] bytes = Files.readAllBytes(path);
            ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(bytes));
            CompressFileStorage.Index map = (CompressFileStorage.Index) is.readObject();
            for (Map.Entry<String, CompressFileStorage.myFile> entry : (Set<Map.Entry<String, CompressFileStorage.myFile>>) map.index.entrySet()) {
                System.out.println("key = " + entry.getKey() + " value = " + entry.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDelete() {
        try {
            storage.save("2", "heiheihei".getBytes());
            storage.save("4", "heiheihei1".getBytes());
            storage.save("5", "heiheihei2".getBytes());
            storage.save("6", "heiheihei3".getBytes());
            storage.save("7", "heiheihei4".getBytes());
            storage.delete("2");
            storage.close();
            showIndex();
        } catch (IOException e) {
            Assert.fail(e.getLocalizedMessage());
        }
    }

    @Test
    public void testGet() {
        try {
            storage.save("3", "heiheihei".getBytes());
            storage.save("4", "heiheihei1".getBytes());
            storage.save("5", "heiheihei2".getBytes());
            storage.save("6", "heiheihei3".getBytes());
            storage.save("7", "heiheihei4".getBytes());
            Assert.assertTrue(Arrays.equals(storage.get("4"), "heiheihei1".getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getLocalizedMessage());
        }
    }


}
