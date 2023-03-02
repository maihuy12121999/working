package rever.rsparkflow.spark.api.configuration;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rever.rsparkflow.spark.api.exception.InvalidConfigError;
import rever.rsparkflow.spark.domain.ExecutionDateInfo;

import java.io.Serializable;
import java.util.*;

/**
 * @author anhlt
 */
public interface Config extends Serializable {

    Map<String, String> getSparkConfigMap();

    SparkConf getSparkConfig();

    String getJobId() throws Exception;

    String getDeployMode();

    boolean isProductionMode();

    Optional<Long> getPrevSuccessExecutionTime();

    Optional<Long> getPrevExecutionTime();

    Optional<Long> getNextExecutionTime();

    ExecutionDateInfo getExecutionDateInfo() throws Exception;

    String getExecutionDate() throws Exception;

    long getExecutionTime() throws Exception;

    Long getDailyReportTime() throws Exception;

    Long getHourlyReportTime() throws Exception;

    boolean isRunRebuild();

    boolean isSyncAll();

    boolean has(String key);

    boolean hasOneOf(List<String> keys);

    String get(String key) throws Exception;

    String get(String key, String defaultValue);

    String getAndDecodeBase64(String key) throws Exception;

    String getAndDecodeBase64(String key, String defaultValue);

    boolean getBoolean(String key) throws Exception;

    boolean getBoolean(String key, boolean defaultValue);

    int getInt(String key) throws Exception;

    int getInt(String key, int defaultValue);

    long getLong(String key) throws Exception;

    long getLong(String key, long defaultValue);

    double getDouble(String key) throws Exception;

    double getDouble(String key, double defaultValue);

    List<String> getList(String key, String delimiter) throws Exception;

    List<String> getList(String key, String delimiter, List<String> defaultValue);

    Config cloneWith(Map<String, Object> data);

    void printConfig();

    static Builder builder() {
        return new Builder();
    }

    class Builder {
        private static final Logger logger = LoggerFactory.getLogger(Builder.class);
        private final Map<String, ConfigArgument> arguments = new HashMap<>();

        public Builder() {
            arguments.put("RV_FS", new ConfigArgument("RV_FS", false, "local"));
            arguments.put("RV_SPARK_CONFIGS", new ConfigArgument("RV_SPARK_CONFIGS", false));
        }

        public Builder addArgument(String name) {
            return addArgument(name, null, false);
        }

        public Builder addArgument(String name, boolean isRequired) {
            return addArgument(name, null, isRequired);
        }

        public Builder addArgument(String name, Object defaultValue) {
            return addArgument(name, defaultValue, false);
        }

        public Builder addArgument(String name, Object defaultValue, boolean isRequired) {
            arguments.put(name, new ConfigArgument(name, isRequired, defaultValue));
            return this;
        }

        public Builder addClickHouseArguments() {
            addArgument("CH_DRIVER", true);
            addArgument("CH_HOST", true);
            addArgument("CH_PORT", true);
            addArgument("CH_USER_NAME", true);
            addArgument("CH_PASSWORD", true);
            return this;
        }

        public Builder addS3Arguments() {
            addArgument("RV_FS", "s3", false);
            addArgument("RV_S3_ACCESS_KEY", true);
            addArgument("RV_S3_SECRET_KEY", true);
            addArgument("RV_S3_REGION", true);
            addArgument("RV_S3_BUCKET", true);
            addArgument("RV_S3_PARENT_PATH", true);
            return this;
        }

        public Builder enableLocalFileSystem() {
            addArgument("RV_FS", "local", false);
            return this;
        }

        public Builder addDataMappingClientArguments() {
            addArgument("RV_DATA_MAPPING_HOST", true);
            addArgument("RV_DATA_MAPPING_USER", true);
            addArgument("RV_DATA_MAPPING_PASSWORD", true);
            return this;
        }

        public Builder addRapIngestionClientArguments() {
            addArgument("RV_RAP_INGESTION_HOST", true);
            return this;
        }

        public Builder addPushKafkaClientArguments() {
            addArgument("RV_PUSH_KAFKA_HOST", true);
            addArgument("RV_PUSH_KAFKA_SK", true);
            return this;
        }

        public Builder addForceRunArguments() {
            addArgument("forcerun.enabled", false);
            addArgument("forcerun.from_date", false);
            addArgument("forcerun.to_date", false);
            return this;
        }

        public Config build(Map<String, Object> map, boolean loadFromEvn) throws InvalidConfigError {
            setupDefaultArguments();
            Map<String, Object> configMap = new HashMap<>(map);
            resolveConfigValues(configMap, loadFromEvn);
            verifyArguments(configMap);
            DefaultConfig config = new DefaultConfig(configMap);

            config.printConfig();
            return config;
        }

        private void setupDefaultArguments() {
            arguments.put("RV_JOB_ID", new ConfigArgument("RV_JOB_ID", true));
            arguments.put("RV_EXECUTION_DATE", new ConfigArgument("RV_EXECUTION_DATE", true));
            arguments.put("RV_RAP_INGESTION_HOST", new ConfigArgument("RV_RAP_INGESTION_HOST", false));
            arguments.put("RV_DATA_MAPPING_HOST", new ConfigArgument("RV_DATA_MAPPING_HOST", false));
            arguments.put("RV_DATA_MAPPING_USER", new ConfigArgument("RV_DATA_MAPPING_USER", false));
            arguments.put("RV_DATA_MAPPING_PASSWORD", new ConfigArgument("RV_DATA_MAPPING_PASSWORD", false));
        }

        private void resolveConfigValues(Map<String, Object> configMap, boolean loadFromEvn) {
            String[] argumentNames = arguments.keySet().toArray(new String[0]);
            if (loadFromEvn) {
                for (String name : argumentNames) {
                    try {
                        String v = System.getenv(name);
                        if (v == null || v.isEmpty()) {
                            v = System.getProperty(name);
                        }
                        if (v != null && !v.isEmpty()) {
                            configMap.put(name, v);
                        }
                        logger.info(String.format("Load ENV: %s=%s", name, v));
                    } catch (Exception ex) {

                    }
                }
            }

            arguments.values().forEach(argument -> {
                Object value = configMap.get(argument.getName());
                if (value == null && argument.getDefaultValue() != null) {
                    configMap.put(argument.getName(), argument.getDefaultValue());
                }
            });
        }

        private void verifyArguments(Map<String, Object> configMap) throws InvalidConfigError {
            List<ConfigArgument> errorArguments = new ArrayList<>();
            this.arguments.values().forEach(argument -> {
                Object value = configMap.get(argument.getName());
                if (argument.isRequired() && value == null && argument.getDefaultValue() == null) {
                    errorArguments.add(argument);
                }
            });

            if (!errorArguments.isEmpty()) {
                String msg = Arrays.toString(errorArguments.stream().map(ConfigArgument::getName).toArray());
                throw new InvalidConfigError("Config arguments are missing or NULL: " + msg);
            }

        }

    }
}
