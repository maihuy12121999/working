package rever.rsparkflow.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * @author anhlt (andy)
 * @since 30/04/2022
 **/
public class Launcher {
    private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

    public static void main(String[] args) throws Exception {
        if (args == null || args.length <= 0) {
            throw new Exception("Missing arguments. Usage: java -jar your_jar_file.jar main.class [args]");
        }

        Class<?> clazz = Class.forName(args[0]);
        String[] appArgs = getAppArguments(args);

        Method mainMethod = clazz.getDeclaredMethod("main", String[].class);
        logger.info(clazz.toGenericString());
        logger.info(mainMethod.getName() + ": " + mainMethod.toGenericString());
        mainMethod.invoke(null, (Object) appArgs);

    }

    private static String[] getAppArguments(String[] args) {
        String[] appArgs = new String[0];

        if (args.length > 1) {
            appArgs = new String[args.length - 1];
            System.arraycopy(args, 1, appArgs, 0, appArgs.length);
        }

        return appArgs;
    }
}
