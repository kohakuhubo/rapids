package cn.berry.rapids.definition;

import java.util.List;

public record BaseDataDefinitions(List<BaseDataDefinition> baseDataDefinitions) {
    @Override
    public List<BaseDataDefinition> baseDataDefinitions() {
        return baseDataDefinitions;
    }
}
