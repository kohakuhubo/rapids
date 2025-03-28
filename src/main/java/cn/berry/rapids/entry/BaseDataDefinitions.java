package cn.berry.rapids.entry;

import java.util.List;

public record BaseDataDefinitions(List<BaseDataDefinition> baseDataDefinitions) {
    @Override
    public List<BaseDataDefinition> baseDataDefinitions() {
        return baseDataDefinitions;
    }
}
