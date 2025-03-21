package cn.berry.rapids.configuration;

public class SourceConfig {

    private KafkaConfig kafka;

    public KafkaConfig getKafka() {
        return kafka;
    }

    public void setKafka(KafkaConfig kafka) {
        this.kafka = kafka;
    }
}
