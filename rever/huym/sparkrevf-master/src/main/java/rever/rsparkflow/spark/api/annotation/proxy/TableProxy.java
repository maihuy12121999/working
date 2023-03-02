package rever.rsparkflow.spark.api.annotation.proxy;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import rever.rsparkflow.spark.api.SinkWriter;
import rever.rsparkflow.spark.api.SourceReader;
import rever.rsparkflow.spark.api.annotation.Output;
import rever.rsparkflow.spark.api.annotation.Table;
import rever.rsparkflow.spark.api.configuration.Config;
import rever.rsparkflow.spark.api.exception.SinkWriterNotFoundError;
import rever.rsparkflow.spark.api.exception.SourceReaderNotFoundError;
import rever.rsparkflow.spark.utils.ReflectionUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author anhlt
 */
public final class TableProxy {
    private final String tableName;
    private final Method buildTableMethod;
    private final Map<String, Class<? extends SourceReader>> tablesToReaderMap;
    private final List<Class<? extends SinkWriter>> tableSinkWriters;

    private TableProxy(final Method btm) {
        validateBuildMethod(btm);
        this.buildTableMethod = btm;
        this.tableName = TableAnnotation.ofMethod(btm).getName();
        validateTableName(tableName);
        this.tablesToReaderMap = getDependencies(btm);
        this.tableSinkWriters = Arrays.stream(btm.getAnnotationsByType(Output.class))
                .map(Output::writer)
                .filter(clazz -> clazz != SinkWriter.NoopSinkWriter.class)
                .collect(Collectors.toList());

        Collections.reverse(tableSinkWriters);
    }

    public Dataset<Row> processTable(Map<String, Dataset<Row>> builtTables, Map<Class<? extends SourceReader>, SourceProxy> tableReaders, Map<Class<? extends SinkWriter>, SinkProxy> tableWriters, Config config) throws InstantiationException, IllegalAccessException, InvocationTargetException, SinkWriterNotFoundError {
        Dataset[] dependencies = getDatasetDependencies(builtTables, tableReaders, config);
        List<Object> args = new ArrayList<>(Arrays.asList(dependencies));
        args.add(config);
        Object flowObject = buildTableMethod.getDeclaringClass().newInstance();

        Dataset<Row> df = (Dataset<Row>) ReflectionUtils.invokeUnorderedArgs(buildTableMethod, flowObject, args.toArray());

        Dataset<Row> lastWrittenTableDf = null;
        for (Class<? extends SinkWriter> clazz : tableSinkWriters) {
            SinkProxy sinkProxy = tableWriters.get(clazz);
            if (sinkProxy == null) {
                throw new SinkWriterNotFoundError(clazz.getName());
            }
            lastWrittenTableDf = sinkProxy.write(tableName, df, config);
        }

        Dataset<Row> resultTable = ObjectUtils.firstNonNull(lastWrittenTableDf, df);
        builtTables.put(tableName, resultTable);
        return resultTable;
    }


    private Dataset[] getDatasetDependencies(Map<String, Dataset<Row>> builtTables,
                                             Map<Class<? extends SourceReader>, SourceProxy> tableReaders,
                                             Config config) {

        return tablesToReaderMap.entrySet().stream().map(entry -> {
            String tn = entry.getKey();
            Class<? extends SourceReader> dependencyTableReaderClazz = entry.getValue();
            if (builtTables.containsKey(tn)) {
                return builtTables.get(tn);
            } else {
                try {
                    SourceProxy sourceProxy = tableReaders.get(dependencyTableReaderClazz);
                    if (sourceProxy == null) {
                        throw new SourceReaderNotFoundError(dependencyTableReaderClazz.toGenericString());
                    }
                    return sourceProxy.read(tn, config);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).toArray(Dataset[]::new);
    }

    public static TableProxy ofMethod(Method buildTable) {
        return new TableProxy(buildTable);
    }

    public Set<String> getDependencyTablesNames() {
        return tablesToReaderMap.keySet();
    }

    private static Map<String, Class<? extends SourceReader>> getDependencies(final Method buildTable) {
        Parameter[] pa = buildTable.getParameters();
        Annotation[][] pas = buildTable.getParameterAnnotations();

        return IntStream.range(0, pas.length).filter(i -> {
                    Annotation[] parameterAnnotations = pas[i];
                    return Arrays.stream(parameterAnnotations).anyMatch(a -> Objects.equals(a.annotationType(), Table.class));
                })
                .boxed()
                .collect(Collectors.toMap(
                        i -> TableAnnotation.ofParameter(pa[i]).getName(),
                        i -> TableAnnotation.ofParameter(pa[i]).getReader(),
                        (x, y) -> y,
                        LinkedHashMap::new
                ));
    }

    private static void validateBuildMethod(Method buildMethod) {
        if (buildMethod.getReturnType() != Dataset.class) {
            throw new IllegalArgumentException("The return type of a build method should be org.apache.spark.sql.DataFrame");
        }
    }

    private static void validateTableName(final String tableName) {
        Validate.notNull(tableName);
        Validate.notEmpty(tableName.trim());
    }

    @Override
    public int hashCode() {
        return tableName.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj, "buildMethod");
    }

    @Override
    public String toString() {
        return "Table{" +
                "tableName='" + tableName + '\'' +
                '}';
    }
}
