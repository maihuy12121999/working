package rever.rsparkflow.spark.api.exception;

/**
 * @author anhlt
 */
public class InvalidConfigError extends Exception {
    private static final long serialVersionUID = 889598582716588380L;

    public InvalidConfigError(String msg) {
        super(msg);
    }
}
