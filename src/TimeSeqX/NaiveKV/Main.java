package TimeSeqX.NaiveKV;

import java.io.File;
import java.io.IOException;

public class Main {
    private static final int size = 10_000;
    private static final String filename = "naive.kv";

    NaiveKV kv;

    private void deleteFile() {
        File f = new File(filename);
        boolean ignore = f.delete();
    }

    private void init() throws IOException {
        System.out.print("打开数据库，");
        long startTime = System.currentTimeMillis();
        kv = new NaiveKV(filename);
        long endTime = System.currentTimeMillis();
        System.out.println("耗时：" + (endTime - startTime) + "ms");
    }

    private void insert() throws IOException {
        System.out.print("插入" + size + "条数据，");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            kv.put("" + i, "i=" + i);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("耗时：" + (endTime - startTime) + "ms");
    }

    private void randomRead() throws IOException {
        System.out.print("随机读取" + size + "条数据，");
        long startTime = System.currentTimeMillis();
        int[] randomNums = new int[size];
        for (int i = 0; i < size; i++) {
            randomNums[i] = (int) Math.floor(Math.random() * size);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("生成随机数耗时：" + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        int percent = (int)Math.ceil(size / 100.0);
        for (int i = 0; i < size; i++) {
            String result = kv.get("" + randomNums[i]);
            if (i == 50 * percent) {
                System.out.println("读取" + i / percent + "%, key: " + randomNums[i] + ", value: " + result);
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("随机读取耗时：" + (endTime - startTime) + "ms");
    }

    private void test() throws IOException {
        deleteFile();
        init();
        insert();
        kv.flush();
        System.out.println("分片数：" + kv.getSegmentCount());
        randomRead();
        kv.reorganize();
        System.out.println("分片数：" + kv.getSegmentCount());
        randomRead();
        kv.close();
    }

    public static void main(String[] args) throws IOException {
        new Main().test();
    }
}
