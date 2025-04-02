/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.berry.rapids.data.source.kafka;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Kafka主题分区类
 * 
 * 描述: 封装Kafka主题分区的信息。
 * 
 * 特性:
 * 1. 存储主题名称
 * 2. 存储分区号
 * 
 * @author Berry
 * @version 1.0.0
 */
public final class KafkaTopicPartition implements Serializable {

    private static final long serialVersionUID = 722083576322742325L;

    // ------------------------------------------------------------------------

    private final String topic;
    private final int partition;
    private final int cachedHash;

    /**
     * 构造Kafka主题分区
     * 
     * @param topic 主题名称
     * @param partition 分区号
     */
    public KafkaTopicPartition(String topic, int partition) {
        this.topic = requireNonNull(topic);
        this.partition = partition;
        this.cachedHash = 31 * topic.hashCode() + partition;
    }

    // ------------------------------------------------------------------------

    /**
     * 获取主题名称
     * 
     * @return 主题名称
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 获取分区号
     * 
     * @return 分区号
     */
    public int getPartition() {
        return partition;
    }

    // ------------------------------------------------------------------------

    /**
     * 获取字符串表示
     * 
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return "KafkaTopicPartition{" + "topic='" + topic + '\'' + ", partition=" + partition + '}';
    }

    /**
     * 检查是否相等
     * 
     * @return 是否相等
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o instanceof KafkaTopicPartition) {
            KafkaTopicPartition that = (KafkaTopicPartition) o;
            return this.partition == that.partition && this.topic.equals(that.topic);
        } else {
            return false;
        }
    }

    /**
     * 获取哈希码
     * 
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return cachedHash;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * 获取字符串表示
     * 
     * @param map 主题分区映射
     * @return 字符串表示
     */
    public static String toString(Map<KafkaTopicPartition, Long> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<KafkaTopicPartition, Long> p : map.entrySet()) {
            KafkaTopicPartition ktp = p.getKey();
            sb.append(ktp.getTopic())
                    .append(":")
                    .append(ktp.getPartition())
                    .append("=")
                    .append(p.getValue())
                    .append(", ");
        }
        return sb.toString();
    }

    /**
     * 获取字符串表示
     * 
     * @param partitions 主题分区列表
     * @return 字符串表示
     */
    public static String toString(List<KafkaTopicPartition> partitions) {
        StringBuilder sb = new StringBuilder();
        for (KafkaTopicPartition p : partitions) {
            sb.append(p.getTopic()).append(":").append(p.getPartition()).append(", ");
        }
        return sb.toString();
    }

    /**
     * 比较器类
     */
    public static class Comparator implements java.util.Comparator<KafkaTopicPartition> {
        /**
         * 比较两个主题分区
         * 
         * @param p1 主题分区1
         * @param p2 主题分区2
         * @return 比较结果
         */
        @Override
        public int compare(KafkaTopicPartition p1, KafkaTopicPartition p2) {
            if (!p1.getTopic().equals(p2.getTopic())) {
                return p1.getTopic().compareTo(p2.getTopic());
            } else {
                return Integer.compare(p1.getPartition(), p2.getPartition());
            }
        }
    }
}
