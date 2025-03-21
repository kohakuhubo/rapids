package cn.berry.rapids;

import java.time.LocalDateTime;
import java.util.*;

public class NetworkSpeedCalculator {
    public static Map<Long, List<Double>> calculateSpeeds(List<Pair<Long, Long>> data) {
        // 存储结果
        Map<Long, List<Double>> results = new HashMap<>();

        for (int i = 0; i < data.size() - 1; i++) {
            Pair<Long, Long> current = data.get(i);
            Pair<Long, Long> next = data.get(i + 1);

            long timeDiffMs = next.getKey() - current.getKey();  // 时间差，毫秒
            if (timeDiffMs > 10 * 1000 || timeDiffMs <= 0) continue;  // 只考虑小于等于10秒的时间间隔

            long bytesDiff = next.getValue() - current.getValue();
            double speedBps = bytesDiff * 8.0 / timeDiffMs * 1000;  // 计算速度，bps

            // 确定当前记录属于哪个分钟
            long minute = current.getKey() / (60 * 1000);

            results.putIfAbsent(minute, new ArrayList<>());
            results.get(minute).add(speedBps);
        }

        return results;
    }

    public static void main(String[] args) {
        // 模拟数据
        List<Pair<Long, Long>> networkData = Arrays.asList(
                new Pair<>(1633046400000L, 1000L), // 2021-10-01 00:00:00
                new Pair<>(1633046405000L, 2000L), // 2021-10-01 00:00:05
                new Pair<>(1633046410000L, 3000L), // 2021-10-01 00:00:10
                new Pair<>(1633046415000L, 4000L), // 2021-10-01 00:00:15
                new Pair<>(1633046420000L, 5000L)
        );

        Map<Long, List<Double>> speeds = calculateSpeeds(networkData);
        System.out.println(speeds);

        List<LocalDateTime> minuteCollection = generateMinuteCollection();

        System.out.println("过去1小时的分钟集合：");
        for (LocalDateTime minute : minuteCollection) {
            System.out.println(minute);
        }

        List<Double> speedsY = Arrays.asList(
                1000.0, 2190.0, 1200.0, 3700.0, 0.0, 6000.0, 300.0, 8000.0, 9000.0, 300000000.0
        );

        List<String> formattedAxis = formatSpeedAxis(speedsY, 10); // 5个刻度

        System.out.println("速度刻度：");
        for (String axisLabel : formattedAxis) {
            System.out.println(axisLabel);
        }
    }

    static class Pair<K, V> {
        private K key;
        private V value;

        public Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

    public static List<String> formatSpeedAxis(List<Double> speeds, int numIntervals) {
        if (speeds == null || speeds.isEmpty() || numIntervals <= 0) {
            throw new IllegalArgumentException("Invalid input");
        }

        double maxSpeed = Collections.max(speeds);

        // 确保最大刻度大于等于速度集合的最大值
        double maxAxisValue = Math.ceil(maxSpeed / 1000.0) * 1000.0;

        double range = maxAxisValue;
        double interval = range / (numIntervals - 1);

        // 选择合适的单位
        String unit = chooseUnit(maxAxisValue);
        double unitFactor = getUnitFactor(unit);

        List<String> axisLabels = new ArrayList<>();
        for (int i = 0; i < numIntervals; i++) {
            double value = i * interval;
            double roundedValue = Math.round(value / unitFactor) * unitFactor;
            axisLabels.add(formatSpeed(roundedValue, unitFactor, unit));
        }

        return axisLabels;
    }

    private static String chooseUnit(double maxSpeed) {
        if (maxSpeed >= 1000000) {
            return "Mbps";
        } else if (maxSpeed >= 1000) {
            return "Kbps";
        } else {
            return "bps";
        }
    }

    private static double getUnitFactor(String unit) {
        switch (unit) {
            case "Mbps":
                return 1000000;
            case "Kbps":
                return 1000;
            default:
                return 1;
        }
    }

    private static String formatSpeed(double speed, double unitFactor, String unit) {
        double value = speed / unitFactor;
        return String.format("%.0f %s", value, unit);
    }

    public static List<LocalDateTime> generateMinuteCollection() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime lastMinute = now.minusMinutes(now.getSecond() > 0 ? 1 : 0).withSecond(0).withNano(0);

        List<LocalDateTime> minuteCollection = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            minuteCollection.add(lastMinute.minusMinutes(i));
        }

        // 确保列表按时间降序排列
        minuteCollection.sort(LocalDateTime::compareTo);

        return minuteCollection;
    }
}
