package rever.rsparkflow.spark;

import org.apache.commons.collections4.iterators.IteratorIterable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rever.rsparkflow.spark.api.SinkWriter;
import rever.rsparkflow.spark.api.SourceReader;
import rever.rsparkflow.spark.api.annotation.proxy.SinkProxy;
import rever.rsparkflow.spark.api.annotation.proxy.SourceProxy;
import rever.rsparkflow.spark.api.annotation.proxy.TableProxy;
import rever.rsparkflow.spark.api.configuration.Config;
import rever.rsparkflow.spark.api.exception.SinkWriterNotFoundError;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author anhlt
 * //TODO: Fixed if there is no writer was defined. Do not start the flow.
 */
public class FlowDAG {
    private static final Logger logger = LoggerFactory.getLogger(FlowDAG.class);

    private final Config config;
    private final Map<Class<? extends SourceReader>, SourceProxy> readerMap;
    private final Map<Class<? extends SinkWriter>, SinkProxy> writerMap;
    private final Graph<TableProxy, DefaultEdge> graph;

    public FlowDAG(Map<String, TableProxy> nameToTableMap,
                   Map<Class<? extends SourceReader>, SourceProxy> readerMap,
                   Map<Class<? extends SinkWriter>, SinkProxy> writerMap,
                   Config config) {
        this.config = config;
        this.readerMap = readerMap;
        this.writerMap = writerMap;
        this.graph = getDependencyGraph(nameToTableMap);
    }

    public void buildTables() throws InvocationTargetException, InstantiationException, IllegalAccessException, SinkWriterNotFoundError {
        Iterator<TableProxy> topologicalOrderIterator = new TopologicalOrderIterator<>(graph);
        Map<String, Dataset<Row>> builtTables = new HashMap<>();

        for (TableProxy table : new IteratorIterable<>(topologicalOrderIterator)) {
            table.processTable(builtTables, readerMap, writerMap, config);
        }
    }

    private static Graph<TableProxy, DefaultEdge> getDependencyGraph(final Map<String, TableProxy> tableNameToTable) {
        Graph<TableProxy, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);
        tableNameToTable.values().forEach(graph::addVertex);

        tableNameToTable.forEach((currentTableName, table) -> {
            table.getDependencyTablesNames().forEach(dependencyTableName -> {
                if (tableNameToTable.containsKey(dependencyTableName)) {
                    logger.info("Internal table '{}' <--------- internal table '{}'", currentTableName, dependencyTableName);
                    graph.addEdge(tableNameToTable.get(dependencyTableName), table);
                } else {
                    logger.info("Internal table '{}' <--------- external table '{}'", currentTableName, dependencyTableName);
                }
            });
        });

        return graph;
    }
}
