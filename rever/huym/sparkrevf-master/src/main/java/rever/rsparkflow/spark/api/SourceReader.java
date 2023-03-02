package rever.rsparkflow.spark.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import rever.rsparkflow.spark.api.configuration.Config;

import java.io.Serializable;

/**
 * @author anhlt - aka andy
 **/
public interface SourceReader extends Serializable {

    Dataset<Row> read(String tableName, Config config);

    class NoopSourceReader implements SourceReader {
        private static final long serialVersionUID = -8757581112793652657L;

        @Override
        public Dataset<Row> read(String tableName, Config config) {
            return null;
        }
    }
}

