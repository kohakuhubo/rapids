package cn.berry.rapids.eventbus;

import com.berry.clickhouse.tcp.client.data.Block;

public class BlockEvent implements Event<Block> {

    private final String type;

    private final Block block;

    public BlockEvent(String type, Block block) {
        this.type = type;
        this.block = block;
    }

    @Override
    public String type() {
        return this.type;
    }

    @Override
    public Block getMessage() {
        return this.block;
    }
}
