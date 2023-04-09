package TimeSeqX.NaiveKV;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        int size = 5_000;
        String filename = "naive.kv";
        File f = new File(filename);
        boolean ignore = f.delete();

        System.out.println("打开数据库，如果是第一次运行，会自动创建数据库文件");
        System.out.println("如果不是第一次运行，则会重整数据库分片");
        long startTime = System.currentTimeMillis();
        NaiveKV kv = new NaiveKV(filename);
        long endTime = System.currentTimeMillis();
        System.out.println("耗时：" + (endTime - startTime) + "ms");

        if (kv.get("1") == null) {
            System.out.print("第一次运行，插入" + size + "条数据，");
            startTime = System.currentTimeMillis();
            for (int i = 0; i < size; i++) {
                kv.put("" + i, "i^2=" + i * i);
            }
            endTime = System.currentTimeMillis();
            System.out.println("耗时：" + (endTime - startTime) + "ms");
        }

        System.out.print("随机读取" + size + "条数据，");
        startTime = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            kv.get("" + (int)Math.floor(Math.random() * 1_0000));
            // System.out.println("i=" + i + " " + kv.get("" + i));
        }
        endTime = System.currentTimeMillis();
        System.out.println("耗时：" + (endTime - startTime) + "ms");

        kv.close();
    }
}
