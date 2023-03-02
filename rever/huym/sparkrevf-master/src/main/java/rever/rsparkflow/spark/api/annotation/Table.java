package rever.rsparkflow.spark.api.annotation;

import rever.rsparkflow.spark.api.SourceReader;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author anhlt
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER})
public @interface Table {

    String value() default "";

    String name() default "";

    /**
     * @see SourceReader
     */
    Class<? extends SourceReader> reader() default SourceReader.NoopSourceReader.class;


}
