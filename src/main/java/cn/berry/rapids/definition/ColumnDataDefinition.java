package cn.berry.rapids.definition;

public class ColumnDataDefinition {

    private String name;

    private String type;

    private Class<?> clazz;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) throws ClassNotFoundException {
        this.type = type;
        this.clazz = Class.forName(type);
    }

    public Class<?> getClazz() {
        return clazz;
    }
}
