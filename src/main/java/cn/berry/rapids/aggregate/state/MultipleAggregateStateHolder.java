package cn.berry.rapids.aggregate.state;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class MultipleAggregateStateHolder {

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    private int fileStatePoint;

    private final String fileName;

    private final int limit;

    private static final int DATA_SIZE = 33;

    private static final int FLAG_DATA = 1;

    private static final int FLAG_EOF = 2;

    private static final String PATH = "data/aggregate/";

    private final int totalLength;

    private AggregateStateCache aggregateStateCache;

    public MultipleAggregateStateHolder(String type, int limit) {
        this.fileName = type + ".state";
        this.totalLength = limit * DATA_SIZE;
        this.aggregateStateCache = new AggregateStateCache();
        this.limit = limit;
    }

    public void init() throws IOException {
        URL url = MultipleAggregateStateHolder.class.getProtectionDomain().getCodeSource().getLocation();
        String filePath = URLDecoder.decode(url.getPath(), StandardCharsets.UTF_8);
        filePath = filePath.substring(0, filePath.lastIndexOf(File.pathSeparator) + 1);

        File file = new File(filePath + PATH + this.fileName);
        File parentFile = file.getParentFile();
        if (!parentFile.exists() && !parentFile.mkdirs()) {
            throw new RuntimeException();
        }

        if (!file.exists() && !file.createNewFile()) {
            throw new RuntimeException();
        }

        Path path = Paths.get(file.getParent(), this.fileName);
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, totalLength);
        int offset = 0;
        long end = file.length() - offset;
        byte[] bytes = new byte[this.totalLength];
        this.mappedByteBuffer.get(bytes, offset, bytes.length);
        int position = 0;

        Map<Long, AggregateState> stateMap = new HashMap<>(this.limit);
        boolean loaded = false;
        while ((offset < end) && !loaded) {

        }
    }

    private static class AggregateStateCache {
        private final TreeMap<Long, AggregateState> stateMap = new TreeMap<>(Comparator.reverseOrder());

        public void put(AggregateState state) {
            stateMap.put(state.getEndRowId(), state);
        }

        public AggregateState get(long endRowId) {
            return stateMap.get(endRowId);
        }

        public AggregateState top() {
            if (!stateMap.isEmpty()) {
                return stateMap.firstEntry().getValue();
            } else {
                return null;
            }
        }

        public void removeLast() {
            if (!stateMap.isEmpty()) {
                stateMap.pollLastEntry();
            }
        }

        public int size() {
            return stateMap.size();
        }
    }

}
