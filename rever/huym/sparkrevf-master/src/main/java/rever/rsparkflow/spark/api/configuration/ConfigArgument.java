package rever.rsparkflow.spark.api.configuration;

/**
 * @author anhlt (andy)
 * @since 27/04/2022
 **/
public class ConfigArgument {
    private String name;
    private boolean required;
    private Object defaultValue;
    public ConfigArgument(String name, boolean required) {
        this(name, required, null);
    }

    public ConfigArgument(String name, boolean required, Object defaultValue) {
        this.name = name;
        this.required = required;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public boolean isRequired() {
        return required;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }
}
