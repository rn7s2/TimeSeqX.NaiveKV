package TimeSeqX.NaiveKV;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        NaiveKV kv = new NaiveKV("1.kv");

//        for (int i = 0; i < 1_0000; i++) {
//            String randString = String.valueOf(Math.random());
//            kv.put("key" + i, randString);
//        }
//
//        System.out.println(kv.getSegmentCount());

        //kv.put("你好", "世界");
        //System.out.println(kv.get("你好"));
        //kv.delete("你好");
        //System.out.println(kv.get("你好"));

//        for (int i = 0; i < 1_0000; i++) {
//            System.out.println("i=" + i + " " + kv.get("key" + i));
//        }

        kv.close();
    }
}
