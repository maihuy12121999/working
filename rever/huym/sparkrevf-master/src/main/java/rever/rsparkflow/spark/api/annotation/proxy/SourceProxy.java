package rever.rsparkflow.spark.api.annotation.proxy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import rever.rsparkflow.spark.api.SourceReader;
import rever.rsparkflow.spark.api.configuration.Config;
import rever.rsparkflow.spark.api.exception.InvalidTableReaderError;

/**
 * @author anhlt
 */
public class SourceProxy {
    private final Class<? extends SourceReader> clazz;
    private final SourceReader readerObject;

    private SourceProxy(Class<? extends SourceReader> clazz) {
        this.clazz = clazz;
        try {
            this.readerObject = clazz.newInstance();
        } catch (Exception e) {
            throw new InvalidTableReaderError(e);
        }
    }

    public Dataset<Row> read(String tableName, Config config) {
        try {
            return readerObject.read(tableName, config);
        } catch (Exception e) {
            throw new InvalidTableReaderError(e);
        }
    }

    public Class<? extends SourceReader> getClazz() {
        return clazz;
    }

    public static SourceProxy ofClazz(Class<? extends SourceReader> clazz) {
        return new SourceProxy(clazz);
    }
}