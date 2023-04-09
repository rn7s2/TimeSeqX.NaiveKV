package TimeSeqX.NaiveKV;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * NanoKV 是一个简单的 Key-Value 数据库。
 * <br />
 * 内存格式如下：
 * <br />
 * 开头 4 个字节，表示数据库目前的最大片段容量
 * 接下来表示每个片段，格式如下：
 * <br />
 * key[ASCII code 31]value[ASCII code 30]key[ASCII code 31]value[ASCII code 30]...
 * key[ASCII code 31]value[8 个字节，表示本片段的字节数（包括这个数字的 8 字节）]
 * <br />
 * 其中如果 value 中包含 ASCII code 26，表示这条记录无效，已经被标记为删除。
 */
public final class NaiveKV implements Closeable {
    private static final String KV_SEPERATOR = "" + (char) 31; // KV分隔符，用于分隔键和值
    private static final String RECORD_SEPERATOR = "" + (char) 30; // 记录分隔符，用于分隔键值对
    private static final String TOMBSTONE = "" + (char) 26; // 墓碑，表示键值对已被删除
    private static final int SEGMENT_MAX_COUNT = 8; // 数据库的最大片段数量
    private static final int SEGMENT_MAX_CAPACITY_START = 1024; // 数据库的最大片段容量
    private int segment_max_capacity = 1024; // 目前最大片段容量

    private final RandomAccessFile file; // 数据库文件
    private final TreeMap<String, String> segment = new TreeMap<>(); // 当前缓冲的片段

    public NaiveKV(String filePath) throws IOException {
        this.file = new RandomAccessFile(filePath, "rws");

        // 获取数据库目前的最大片段容量
        if (file.length() != 0) {
            segment_max_capacity = file.readInt(); // 最大片段容量
            reorganize();
        } else {
            file.writeInt(segment_max_capacity); // 最大片段容量
        }
    }

    public String get(String key) throws IOException {
        // 若最新的片段中有该键，则直接返回
        if (segment.containsKey(key)) {
            String ret = segment.get(key);
            if (ret.contains(TOMBSTONE)) {
                return null;
            }
            return ret;
        }

        // 若最新的片段中没有该键，则从文件中查找
        if (file.length() <= Integer.BYTES) {
            return null; // 文件为空
        }
        file.seek(file.length() - Long.BYTES); // 定位到最后一个片段的大小部分之前

        // 从最后一个片段开始，向前在每一个片段中查找
        while (true) {
            // 读取本片段的内容
            RawSegment segment = readSegment();

            // 在本片段中二分查找键
            int i = 0, j = segment.parts.length - 1;
            while (i < j) {
                int m = (i + j) / 2;
                String[] kv = segment.parts[m].split(KV_SEPERATOR);
                int cmp = kv[0].compareTo(key);
                if (cmp == 0) {
                    i = m;
                    break;
                } else if (cmp < 0) {
                    i = m + 1;
                } else {
                    j = m - 1;
                }
            }

            // 确定是否找到键值对
            String[] kv = segment.parts[i].split(KV_SEPERATOR);
            if (kv[0].equals(key)) {
                if (kv[1].contains(TOMBSTONE)) {
                    return null; // 墓碑，表明该键值对已被删除
                }
                return kv[1];
            }

            // 若本片段中没有该键，则继续向前查找
            // 定位到上一个片段的大小部分之前
            long lastSegmentOffset = file.getFilePointer() - segment.size;
            if (lastSegmentOffset <= Integer.BYTES) {
                return null; // 已经到达文件头部
            }
            file.seek(lastSegmentOffset);
        }
    }

    public void put(String key, String value) throws IOException {
        segment.put(key, value);

        if (segment.size() >= segment_max_capacity) { // 需要将当前片段写入文件末尾
            file.seek(file.length()); // 定位到文件末尾

            // 将当前片段写入文件
            writeSegment(segment);
            segment.clear();
        }

        // 检查是否需要合并片段
        int segmentCount = getSegmentCount();
        if (segmentCount >= SEGMENT_MAX_COUNT) { // 合并片段
            segment_max_capacity *= 2;
            mergeSegments();
        }
    }

    public void delete(String key) throws IOException {
        put(key, TOMBSTONE);
    }

    // 将当前更改同步到磁盘
    public void flush() throws IOException {
        if (segment.size() != 0) {
            file.seek(file.length()); // 定位到文件末尾

            // 将当前片段写入文件
            writeSegment(segment);
            segment.clear();
        }
    }

    // 计算合适的分片大小，并重新划分片段
    public void reorganize() throws IOException {
        int pairCount = getPairCount();
        segment_max_capacity = Math.max((int) Math.ceil(pairCount / (SEGMENT_MAX_COUNT / 2.0)), SEGMENT_MAX_CAPACITY_START);
        mergeSegments();
    }

    // 获取数据库中的键值对数量
    public int getPairCount() throws IOException {
        if (file.length() <= Integer.BYTES) { // 文件中没有任何片段
            return 0;
        }

        TreeMap<String, String> allPairs = new TreeMap<>(segment);

        // 从最后一个片段开始，合并片段（重新划分片段）
        file.seek(file.length() - Long.BYTES); // 定位到最后一个片段的大小部分之前
        while (true) {
            // 读取本片段的内容
            RawSegment segment = readSegment();

            // 合并片段中的所有键值对
            for (String segmentPart : segment.parts) {
                String[] kv = segmentPart.split(KV_SEPERATOR);
                if (kv[1].contains(TOMBSTONE)) {
                    continue;
                }
                allPairs.putIfAbsent(kv[0], kv[1]);
            }

            // 定位到上一个片段的大小部分之前
            long lastSegmentOffset = file.getFilePointer() - segment.size;
            if (lastSegmentOffset <= Integer.BYTES) {
                break;
            }
            file.seek(lastSegmentOffset);
        }

        return allPairs.size();
    }

    // 获取数据库中的片段数量
    public int getSegmentCount() throws IOException {
        if (file.length() <= Integer.BYTES) { // 文件中没有任何片段
            return segment.size() == 0 ? 0 : 1;
        }

        int cnt = segment.size() == 0 ? 1 : 2;
        file.seek(file.length() - Long.BYTES); // 定位到最后一个片段的大小部分之前

        // 从最后一个片段开始，向前跳转
        while (true) {
            long segmentSize = file.readLong(); // 本片段的大小
            long lastSegmentOffset = file.getFilePointer() - segmentSize - Long.BYTES; // 上一个片段的偏移
            if (lastSegmentOffset <= Integer.BYTES) { // 已经到达文件头部
                return cnt;
            }
            cnt++;
            file.seek(lastSegmentOffset);
        }
    }

    // 片段信息
    private static final class RawSegment {
        public long size;
        public String[] parts;

        public RawSegment(long size, String[] parts) {
            this.size = size;
            this.parts = parts;
        }
    }

    // 读取本片段的内容
    private RawSegment readSegment() throws IOException {
        long segmentSize = file.readLong(); // 本片段的大小
        long segmentOffset = file.getFilePointer() - segmentSize; // 本片段的偏移
        file.seek(segmentOffset); // 定位到本片段内容之前

        // 读取本片段的内容
        byte[] segmentBytes = new byte[(int) segmentSize - Long.BYTES];
        file.read(segmentBytes);
        String segmentString = new String(segmentBytes, StandardCharsets.UTF_16BE);

        return new RawSegment(segmentSize, segmentString.split(RECORD_SEPERATOR));
    }

    // 将片段写入文件
    private void writeSegment(TreeMap<String, String> segment) throws IOException {
        // 将当前片段写入文件
        StringBuilder sb = new StringBuilder();
        int cnt = 0;
        for (String k : segment.keySet()) {
            sb.append(k).append(KV_SEPERATOR).append(segment.get(k));
            if (cnt != segment.size() - 1) {
                sb.append(RECORD_SEPERATOR);
            }
            cnt++;
        }
        byte[] segmentBytes = sb.toString().getBytes(StandardCharsets.UTF_16BE);
        file.write(segmentBytes);
        file.writeLong(segmentBytes.length + Long.BYTES); // 写入本片段的大小
    }

    // 根据当前 segment_max_capacity 重新划分片段
    private void mergeSegments() throws IOException {
        if (file.length() <= Integer.BYTES && segment.size() == 0) { // 文件中没有任何片段
            return;
        }

        ArrayList<TreeMap<String, String>> newSegments = new ArrayList<>();
        newSegments.add(new TreeMap<>(segment));
        int newSegmentCount = 0;

        // 从最后一个片段开始，合并片段（重新划分片段）
        file.seek(file.length() - Long.BYTES); // 定位到最后一个片段的大小部分之前
        while (true) {
            // 读取本片段的内容
            RawSegment segment = readSegment();

            // 合并片段中的所有键值对
            for (String segmentPart : segment.parts) {
                String[] kv = segmentPart.split(KV_SEPERATOR);
                if (kv[1].contains(TOMBSTONE)) {
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
                    newSegments.add(new TreeMap<>());
                }
                newSegments.get(newSegmentCount).put(kv[0], kv[1]);
                if (newSegments.get(newSegmentCount).size() >= segment_max_capacity) {
                    newSegmentCount++;
                }
            }

            // 定位到上一个片段的大小部分之前
            long lastSegmentOffset = file.getFilePointer() - segment.size;
            if (lastSegmentOffset <= Integer.BYTES) {
                break;
            }
            file.seek(lastSegmentOffset);
        }

        // 将新的片段写入文件
        file.setLength(0);
        file.writeInt(segment_max_capacity); // 写入最大片段容量
        Collections.reverse(newSegments); // 反转保证后写入最新的片段（最新片段在文件尾部）
        for (TreeMap<String, String> newSegment : newSegments) {
            writeSegment(newSegment);
        }
    }

    // 刷写后关闭文件，释放资源
    @Override
    public void close() throws IOException {
        flush();
        file.close();
    }
}
