package rever.rsparkflow.spark;

import org.apache.spark.sql.SparkSession;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rever.rsparkflow.spark.api.SinkWriter;
import rever.rsparkflow.spark.api.SourceReader;
import rever.rsparkflow.spark.api.annotation.Table;
import rever.rsparkflow.spark.api.annotation.proxy.SinkProxy;
import rever.rsparkflow.spark.api.annotation.proxy.SourceProxy;
import rever.rsparkflow.spark.api.annotation.proxy.TableAnnotation;
import rever.rsparkflow.spark.api.annotation.proxy.TableProxy;
import rever.rsparkflow.spark.api.configuration.Config;
import rever.rsparkflow.spark.utils.Utils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author anhlt
 */
public class RSparkFlow {
    private static final Logger logger = LoggerFactory.getLogger(RSparkFlow.class);

    private String packageName;
    private Reflections reflections;
    private final Map<String, TableProxy> nameToTableMap = new HashMap<>();
    private final Map<Class<? extends SourceReader>, SourceProxy> readerMap = new HashMap<>();
    private final Map<Class<? extends SinkWriter>, SinkProxy> writerMap = new HashMap<>();

    public void run(String packageName, Config config) throws Exception {
        printSparkConfig();

        this.packageName = packageName;
        this.reflections = null;
        this.nameToTableMap.clear();
        this.readerMap.clear();
        this.writerMap.clear();

        this.reflections = new Reflections(packageName, new SubTypesScanner(false), new MethodAnnotationsScanner());
        collectReaderMap();
        collectWriterMap();
        collectTableMap();

        new FlowDAG(nameToTableMap, readerMap, writerMap, config).buildTables();
    }


    private void collectReaderMap() {
        Map<? extends Class<? extends SourceReader>, SourceProxy> map = reflections.getSubTypesOf(SourceReader.class).stream()
                .filter(clazz -> Utils.isBelongToPackage(packageName, clazz.getName()))
                .filter(clazz -> clazz != SourceReader.NoopSourceReader.class)
                .map(SourceProxy::ofClazz)
                .collect(Collectors.toMap(
                        SourceProxy::getClazz,
                        Function.identity()
                ));

        readerMap.putAll(map);
    }

    private void collectWriterMap() {
        reflections.getSubTypesOf(SinkWriter.class).stream()
                .filter(clazz -> Utils.isBelongToPackage(packageName, clazz.getName()))
                .filter(clazz -> clazz != SinkWriter.NoopSinkWriter.class)
                .map(SinkProxy::ofClazz)
                .collect(Collectors.toMap(
                        SinkProxy::getName,
                        Function.identity()
                )).entrySet().stream().forEach(entry -> {
                    writerMap.put(entry.getKey(), entry.getValue());
                });
    }

    private void collectTableMap() {
        reflections.getMethodsAnnotatedWith(Table.class).stream()
                .filter(methodClazz -> Utils.isBelongToPackage(packageName, methodClazz.getDeclaringClass().getName()))
                .collect(Collectors.toMap(
                        m -> TableAnnotation.ofMethod(m).getName(),
                        Function.identity()
                )).entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> TableProxy.ofMethod(entry.getValue())
                )).entrySet().stream().forEach(entry -> {
                    nameToTableMap.put(entry.getKey(), entry.getValue());
                });
    }

    private void printSparkConfig() {
        logger.info("---------------------------");
        logger.info("Run with these Spark configs:");
        logger.info("---");
        SparkSession.getActiveSession().foreach(sparkSession -> {
            for (Tuple2<String, String> tuple : sparkSession.sparkContext().getConf().getAll()) {
                logger.info(String.format("Spark config: %s=%s", tuple._1, tuple._2));
            }
            return 0;
        });
        logger.info("---------------------------");
    }


}
