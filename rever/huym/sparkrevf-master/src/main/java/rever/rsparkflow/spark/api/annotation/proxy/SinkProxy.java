package rever.rsparkflow.spark.api.annotation.proxy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import rever.rsparkflow.spark.api.SinkWriter;
import rever.rsparkflow.spark.api.configuration.Config;
import rever.rsparkflow.spark.api.exception.InvalidTableWriterError;

/**
 * @author anhlt
 */
public class SinkProxy {
    private final Class<? extends SinkWriter> name;
    private final SinkWriter writer;

    private SinkProxy(Class<? extends SinkWriter> clazz) throws InvalidTableWriterError {
        this.name = clazz;
        try {
            this.writer = name.newInstance();
        } catch (Exception e) {
            throw new InvalidTableWriterError("No required constructor for this writer class", e);
        }
    }

    public Dataset<Row> write(String tableName, Dataset<Row> tableDf, Config config) {
        return writer.write(tableName, tableDf, config);
    }

    public Class<? extends SinkWriter> getName() {
        return name;
    }

    public static SinkProxy ofClazz(Class<? extends SinkWriter> clazz) throws InvalidTableWriterError {
        return new SinkProxy(clazz);
    }
}
