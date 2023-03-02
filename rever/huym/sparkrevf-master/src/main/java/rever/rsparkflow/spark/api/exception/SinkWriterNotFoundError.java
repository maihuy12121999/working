package rever.rsparkflow.spark.api.exception;

/**
 * @author anhlt
 */
//TODO: Use REVER common error base class if it's ok to run on Spark cluster env ( need verify again)
public class SinkWriterNotFoundError extends Exception {
    private static final long serialVersionUID = -1505823368819212054L;

    public SinkWriterNotFoundError(String tableWriterName) {
        super("Sink writer " + tableWriterName + " not found");
    }
}
