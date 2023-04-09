package TimeSeqX.NaiveKV;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        NaiveKV kv = new NaiveKV("1.kv");

//        for (int i = 0; i < 1_0000; i++) {
//            kv.put("key" + i, "value" + i);
//        }

        System.out.println(kv.getSegmentCount());

        System.out.println(kv.get("key9997"));

//        for (int i = 0; i < 1_0000; i++) {
//            System.out.println("i=" + i + " " + kv.get("key" + i));
//        }

        kv.close();
    }
}
