package TimeSeqX.NaiveKV;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * NanoKV 是一个简单的 Key-Value 数据库。
 * <br />
 * 内存格式如下：
 * <br />
 * 开头 4 个字节，表示数据库目前的最大片段容量
 * 接下来表示每个片段，格式如下：
 * <br />
 * key:value[ASCII code 31]key:value[ASCII code 31]...key:value[8 个字节，表示本片段的字节数（包括这个数字的 8 字节）]
 * <br />
 * 其中如果 value 中包含 ASCII code 26，表示这条记录无效，已经被标记为删除。
 */
public final class NaiveKV implements Closeable {
    private static final int SEGMENT_MAX_COUNT = 8;
    private int segment_max_capacity = 1024;

    private final RandomAccessFile file;
    private final HashMap<String, String> segment = new HashMap<>();

    public NaiveKV(String filePath) {
        try {
            this.file = new RandomAccessFile(filePath, "rws");

            // 获取数据库目前的最大片段容量
            if (file.length() != 0) {
                segment_max_capacity = file.readInt(); // 最大片段容量
            } else {
                file.writeInt(segment_max_capacity); // 写入最大片段容量
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String get(String key) {
        try {
            // 若最新的片段中有该键，则直接返回
            if (segment.containsKey(key)) {
                return segment.get(key);
            }

            // 若最新的片段中没有该键，则从文件中查找
            if (file.length() <= Integer.BYTES) {
                return null; // 文件为空
            }

            file.seek(file.length() - Long.BYTES); // 定位到最后一个片段的大小部分之前

            // 从最后一个片段开始，向前在每一个片段中查找
            while (true) {
                long segmentSize = file.readLong(); // 本片段的大小
                long segmentOffset = file.getFilePointer() - segmentSize; // 本片段的偏移
                file.seek(segmentOffset); // 定位到本片段内容之前

                // 读取本片段的内容
                byte[] segmentBytes = new byte[(int) segmentSize - Long.BYTES];
                file.read(segmentBytes);
                String segmentString = new String(segmentBytes, StandardCharsets.UTF_16BE);

                // 在本片段中查找键
                // TODO 改为二分查找
                String[] segmentParts = segmentString.split("" + (char) 31);
                for (String segmentPart : segmentParts) {
                    String[] kv = segmentPart.split(":");
                    if (kv[0].equals(key)) {
                        if (kv[1].contains("" + (char) 26)) {
                            return null;
                        }
                        return kv[1];
                    }
                }

                // 若本片段中没有该键，则继续向前查找
                // 定位到上一个片段的大小部分之前
                long lastSegmentOffset = file.getFilePointer() - segmentSize;
                if (lastSegmentOffset <= Integer.BYTES) {
                    return null; // 已经到达文件头部
                }
                file.seek(lastSegmentOffset);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(String key, String value) {
        try {
            segment.put(key, value);

            if (segment.size() >= segment_max_capacity) { // 需要将当前片段写入文件
                // 写入文件末尾
                file.seek(file.length()); // 定位到文件末尾

                // 将当前片段写入文件
                StringBuilder sb = new StringBuilder();
                int cnt = 0;
                for (String k : segment.keySet()) {
                    sb.append(k).append(":").append(segment.get(k));
                    if (cnt != segment.size() - 1) {
                        sb.append((char) 31);
                    }
                    cnt++;
                }
                byte[] segmentBytes = sb.toString().getBytes(StandardCharsets.UTF_16BE);
                file.write(segmentBytes);
                file.writeLong(segmentBytes.length + Long.BYTES); // 写入本片段的大小

                // 清空当前片段
                segment.clear();
            }

            int segmentCount = getSegmentCount();
            if (segmentCount >= SEGMENT_MAX_COUNT) { // 先进行合并，再进行写入
                segment_max_capacity *= 2;
                ArrayList<HashMap<String, String>> newSegments = new ArrayList<>();
                newSegments.add(new HashMap<>(segment));
                int newSegmentCount = 0;

                // 从文件中读取所有片段
                file.seek(file.length() - Long.BYTES); // 定位到最后一个片段的大小部分之前

                // 从最后一个片段开始，重新划分片段
                while (true) {
                    long segmentSize = file.readLong(); // 本片段的大小
                    long segmentOffset = file.getFilePointer() - segmentSize; // 本片段的偏移
                    file.seek(segmentOffset); // 定位到本片段内容之前

                    // 读取本片段的内容
                    byte[] segmentBytes = new byte[(int) segmentSize - Long.BYTES];
                    file.read(segmentBytes);
                    String segmentString = new String(segmentBytes, StandardCharsets.UTF_16BE);

                    // 保存片段中的所有键值对
                    String[] segmentParts = segmentString.split("" + (char) 31);
                    for (String segmentPart : segmentParts) {
                        String[] kv = segmentPart.split(":");
                        if (kv[1].contains("" + (char) 26)) {
                            continue;
                        }
                        boolean contains = newSegments.stream().reduce(
                                false,
                                (a, b) -> a || b.containsKey(kv[0]),
                                (a, b) -> a || b
                        );
                        if (contains) {
                            continue;
                        }
                        if (newSegments.size() <= newSegmentCount) {
                            newSegments.add(new HashMap<>());
                        }
                        newSegments.get(newSegmentCount).put(kv[0], kv[1]);
                        if (newSegments.get(newSegmentCount).size() >= segment_max_capacity) {
                            newSegmentCount++;
                        }
                    }

                    // 定位到上一个片段的大小部分之前
                    long lastSegmentOffset = file.getFilePointer() - segmentSize;
                    if (lastSegmentOffset <= Integer.BYTES) {
                        break;
                    }
                    file.seek(lastSegmentOffset);
                }

                // 将新的片段写入文件
                file.setLength(0);
                file.writeInt(segment_max_capacity); // 写入最大片段容量

                for (HashMap<String, String> newSegment : newSegments) {
                    // 将当前片段写入文件
                    StringBuilder sb = new StringBuilder();
                    int cnt = 0;
                    for (String k : newSegment.keySet()) {
                        sb.append(k).append(":").append(newSegment.get(k));
                        if (cnt != newSegment.size() - 1) {
                            sb.append((char) 31);
                        }
                        cnt++;
                    }
                    byte[] segmentBytes = sb.toString().getBytes(StandardCharsets.UTF_16BE);
                    file.write(segmentBytes);
                    file.writeLong(segmentBytes.length + Long.BYTES); // 写入本片段的大小
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getSegmentCount() {
        try {
            if (file.length() <= Integer.BYTES) { // 文件为空
                if (segment.size() == 0) {
                    return 0;
                } else {
                    return 1;
                }
            }

            int cnt;
            if (segment.size() == 0) {
                cnt = 1;
            } else {
                cnt = 2;
            }
            file.seek(file.length() - Long.BYTES); // 定位到最后一个片段的大小部分之前

            // 从最后一个片段开始，向前跳转
            while (true) {
                long segmentSize = file.readLong(); // 本片段的大小
                long lastSegmentOffset = file.getFilePointer() - segmentSize - Long.BYTES; // 上一个片段的偏移
                if (lastSegmentOffset <= Integer.BYTES) {
                    return cnt; // 已经到达文件头部
                }
                cnt++;
                file.seek(lastSegmentOffset);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() throws IOException {
        if (segment.size() != 0) {
            file.seek(file.length()); // 定位到文件末尾

            // 将当前片段写入文件
            StringBuilder sb = new StringBuilder();
            int cnt = 0;
            for (String k : segment.keySet()) {
                sb.append(k).append(":").append(segment.get(k));
                if (cnt != segment.size() - 1) {
                    sb.append((char) 31);
                }
                cnt++;
            }
            byte[] segmentBytes = sb.toString().getBytes(StandardCharsets.UTF_16BE);
            file.write(segmentBytes);
            file.writeLong(segmentBytes.length + Long.BYTES); // 写入本片段的大小

            segment.clear();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        file.close();
    }
}
