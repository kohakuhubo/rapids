package cn.berry.rapids.configuration;

import java.util.List;

public class KafkaConfig {

    private String appName;

    private String bootstrapServers;

    private String groupId;

    private List<String> subTopics;

    private long pollTimeout;

    private long sessionTimeout;

    private long maxPollIntervalMs;

    private String clientId;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public List<String> getSubTopics() {
        return subTopics;
    }

    public void setSubTopics(List<String> subTopics) {
        this.subTopics = subTopics;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public long getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public long getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    public void setMaxPollIntervalMs(long maxPollIntervalMs) {
        this.maxPollIntervalMs = maxPollIntervalMs;
    }

    public String getClientId() {
        return clientId;
    }
}
