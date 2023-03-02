package rever.rsparkflow.spark.utils;

import rever.rsparkflow.spark.api.configuration.Config;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author anhlt
 */
public class ReflectionUtils {
    public static Object invokeUnorderedArgs(Method m, Object obj, Object... args) throws InvocationTargetException, IllegalAccessException {
        Map<String, List<Object>> classNameToArgument = Arrays.stream(args).collect(Collectors.groupingBy(o -> {

            Class<?>[] interfaces = o.getClass().getInterfaces();

            if (o.getClass().isSynthetic()) {
                return interfaces[0].getCanonicalName();
            } else if (interfaces.length > 0 && interfaces[0] == Config.class) {
                return interfaces[0].getCanonicalName();
            } else {
                return o.getClass().getCanonicalName();
            }
        }));
        Object[] orderedArgs = Arrays
                .stream(m.getParameterTypes())
                .map(c -> classNameToArgument.get(c.getCanonicalName()).remove(0))
                .toArray();
        return m.invoke(obj, orderedArgs);
    }
}
