package cn.berry.rapids.definition;

import java.util.List;

public record SourceDataDefinitions(List<SourceDataDefinition> definitions) {
    public List<SourceDataDefinition> definitions() {
        return definitions;
    }
}
