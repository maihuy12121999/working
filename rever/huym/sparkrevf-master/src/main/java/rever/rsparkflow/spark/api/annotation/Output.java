package rever.rsparkflow.spark.api.annotation;

import rever.rsparkflow.spark.api.SinkWriter;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
@Repeatable(MultipleOutput.class)
public @interface Output {
    /**
     * Table writer
     *
     * @see SinkWriter
     */
    Class<? extends SinkWriter> writer() default SinkWriter.NoopSinkWriter.class;
}
