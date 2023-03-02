package rever.rsparkflow.spark.api.annotation.proxy;

import org.apache.commons.lang3.StringUtils;
import rever.rsparkflow.spark.api.SourceReader;
import rever.rsparkflow.spark.api.annotation.Table;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * @author anhlt
 */
public class TableAnnotation {
    private final Table t;

    private TableAnnotation(Table t) {
        this.t = t;
    }

    public String getName() {
        return StringUtils.firstNonBlank(t.name(), t.value());
    }

    public Class<? extends SourceReader> getReader() {
        return t.reader();
    }

    public static TableAnnotation ofMethod(Method method) {
        return new TableAnnotation(method.getAnnotation(Table.class));
    }

    public static TableAnnotation ofParameter(Parameter parameter) {
        return new TableAnnotation(parameter.getAnnotation(Table.class));
    }

    @Override
    public String toString() {
        return "TableAnnotation{" + t + '}';
    }
}
