package rever.rsparkflow.spark.api.exception;

/**
 * @author anhlt
 */
//TODO: Use REVER common error base class if it's ok to run on Spark cluster env ( need verify again)
public class SourceReaderNotFoundError extends Exception {
    private static final long serialVersionUID = -4567876534184453290L;

    public SourceReaderNotFoundError(String tableReaderName) {
        super("Source reader " + tableReaderName + " not found");
    }
}
