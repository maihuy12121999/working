package rever.rsparkflow.spark.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import rever.rsparkflow.spark.api.configuration.Config;

import java.io.Serializable;

/**
 * @author anhlt - aka andy
 **/
public interface SinkWriter extends Serializable {
    Dataset<Row> write(String tableName, Dataset<Row> tableData, Config config);


    class NoopSinkWriter implements SinkWriter {
        private static final long serialVersionUID = 8088164390364736567L;

        @Override
        public Dataset<Row> write(String tableName, Dataset<Row> tableData, Config config) {
            return null;
        }
    }
}
