package rever.rsparkflow.spark.api.exception;

/**
 * @author anhlt
 */
//TODO: Use REVER common error base class if it's ok to run on Spark cluster env ( need verify again)
public class InvalidTableWriterError extends RuntimeException {
    private static final long serialVersionUID = -6724962621547333436L;

    public InvalidTableWriterError(String msg, Exception e) {
        super(msg, e);
    }
}
