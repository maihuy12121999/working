package rever.rsparkflow.spark.api.exception;

/**
 * @author anhlt
 */
//TODO: Use REVER common error base class if it's ok to run on Spark cluster env ( need verify again)
public class InvalidTableReaderError extends RuntimeException {
    private static final long serialVersionUID = -1357402099515168980L;

    public InvalidTableReaderError(Exception e) {
        super(e);
    }
}
