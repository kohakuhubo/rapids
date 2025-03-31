package cn.berry.rapids.configuration;

public class SystemConfig {

    private SourceConfig source;

    private AggregateConfig aggregate;

    private BlockConfig block;

    private DataConfig data;

    private ClickHouseConfig clickHouse;

    public AggregateConfig getAggregate() {
        return aggregate;
    }

    public SourceConfig getSource() {
        return source;
    }

    public void setSource(SourceConfig source) {
        this.source = source;
    }

    public void setAggregate(AggregateConfig aggregate) {
        this.aggregate = aggregate;
    }

    public BlockConfig getBlock() {
        return block;
    }

    public void setBlock(BlockConfig block) {
        this.block = block;
    }

    public DataConfig getData() {
        return data;
    }

    public void setData(DataConfig data) {
        this.data = data;
    }

    public ClickHouseConfig getClickHouse() {
        return clickHouse;
    }

    public void setClickHouse(ClickHouseConfig clickHouse) {
        this.clickHouse = clickHouse;
    }
}
