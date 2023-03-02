package rever.rsparkflow.spark.api.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rever.rsparkflow.spark.domain.ExecutionDateInfo;
import rever.rsparkflow.spark.utils.JsonUtils;
import rever.rsparkflow.spark.utils.TimestampUtils;
import scala.Option;

import java.util.*;

/**
 * @author anhlt
 */
public class DefaultConfig implements Config {

    private static final long serialVersionUID = 1646237206606416817L;
    private static final Logger logger = LoggerFactory.getLogger(DefaultConfig.class);

    private Map<String, Object> data;

    public DefaultConfig(Map<String, Object> data) {
        this.data = data;
    }

    @Override
    public SparkConf getSparkConfig() {
        Map<String, String> configMap = getSparkConfigMap();

        SparkConf sparkConfig = new SparkConf();
        configMap.entrySet().forEach(entry -> {
            sparkConfig.set(entry.getKey(), entry.getValue());
        });
        return sparkConfig;
    }

    @Override
    public Map<String, String> getSparkConfigMap() {

        Map<String, String> configMap = new HashMap<>();

        String configStr = get("RV_SPARK_CONFIGS", "{}");
        JsonNode jsonNode = JsonUtils.toJsonNode(configStr);

        for (Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            String name = entry.getKey();
            JsonNode valueNode = entry.getValue();
            if (!valueNode.isMissingNode() && !valueNode.isNull()) {
                if (valueNode.isContainerNode()) {
                    configMap.put(name, valueNode.toString());
                } else {
                    configMap.put(name, valueNode.asText(""));
                }
            }
        }
        return configMap;
    }


    @Override
    public String getJobId() throws Exception {
        return get("RV_JOB_ID");
    }

    @Override
    public String getDeployMode() {
        return get("RV_MODE", "development");
    }

    /**
     * Check if `RV_MODE` equals to: production
     */
    @Override
    public boolean isProductionMode() {
        return getDeployMode().equalsIgnoreCase("production");
    }

    @Override
    public Optional<Long> getPrevExecutionTime() {
        Option<Object> executionTime = TimestampUtils.parseExecutionTime(get("RV_PREV_EXECUTION_DATE", ""));

        if (executionTime.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(Long.parseLong(executionTime.get().toString()));
        }

    }


    @Override
    public Optional<Long> getPrevSuccessExecutionTime() {
        Option<Object> executionTime = TimestampUtils.parseExecutionTime(get("RV_PREV_SUCCESS_EXECUTION_DATE", ""));

        if (executionTime.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(Long.parseLong(executionTime.get().toString()));
        }

    }

    @Override
    public Optional<Long> getNextExecutionTime() {
        Option<Object> executionTime = TimestampUtils.parseExecutionTime(get("RV_NEXT_EXECUTION_DATE", ""));

        if (executionTime.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(Long.parseLong(executionTime.get().toString()));
        }

    }


    @Override
    public String getExecutionDate() throws Exception {
        return get("RV_EXECUTION_DATE");
    }

    @Override
    public long getExecutionTime() throws Exception {
        Option<Object> executionTime = TimestampUtils.parseExecutionTime(getExecutionDate());

        if (executionTime.isEmpty()) {
            throw new Exception("RV_EXECUTION_DATE: is invalid.");
        }

        return Long.parseLong(executionTime.get().toString());

    }


    @Override
    public Long getDailyReportTime() throws Exception {
        return TimestampUtils.asStartOfDay(getExecutionTime());
    }

    @Override
    public Long getHourlyReportTime() throws Exception {
        return TimestampUtils.asStartOfHour(getExecutionTime());
    }

    @Override
    public ExecutionDateInfo getExecutionDateInfo() throws Exception {
        return ExecutionDateInfo.apply(
                getExecutionTime(),
                getPrevSuccessExecutionTime().isPresent() ? Option.apply(getPrevSuccessExecutionTime().get()) : Option.apply((Long) null),
                getPrevExecutionTime().isPresent() ? Option.apply(getPrevExecutionTime().get()) : Option.apply((Long) null),
                getNextExecutionTime().isPresent() ? Option.apply(getNextExecutionTime().get()) : Option.apply((Long) null)
        );
    }

    @Override
    public boolean isRunRebuild() {
        return getBoolean("run_rebuild", false);
    }

    public boolean isSyncAll() {
        return getBoolean("is_sync_all", false);
    }

    @Override
    public boolean has(String key) {
        return data.containsKey(key);
    }

    @Override
    public boolean hasOneOf(List<String> keys) {
        return keys.stream().anyMatch(this::has);
    }

    @Override
    public List<String> getList(String key, String delimiter) throws Exception {
        String[] fields = get(key).split(delimiter, -1);
        return Arrays.asList(fields);
    }

    @Override
    public List<String> getList(String key, String delimiter, List<String> defaultValue) {
        try {
            return getList(key, delimiter);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    @Override
    public String get(String key) throws Exception {
        Object value = data.get(key);
        if (value == null) {
            throw new Exception("No key was found:" + key);
        }
        return value.toString();
    }

    @Override
    public String get(String key, String defaultValue) {
        try {
            return get(key);
        } catch (Exception ex) {
            return defaultValue;
        }
    }


    @Override
    public String getAndDecodeBase64(String key) throws Exception {
        Object value = data.get(key);
        if (value == null) {
            throw new Exception("No key was found:" + key);
        }
        String base64 = new String(Base64.getDecoder().decode(value.toString()), "UTF-8");
        return base64;
    }

    @Override
    public String getAndDecodeBase64(String key, String defaultValue) {
        try {
            return getAndDecodeBase64(key);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    @Override
    public boolean getBoolean(String key) throws Exception {
        Object value = data.get(key);
        if (value == null) {
            throw new Exception("No key was found:" + key);
        }
        return Boolean.parseBoolean(value.toString());
    }

    @Override
    public boolean getBoolean(String key, boolean defaultValue) {
        try {
            return getBoolean(key);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    @Override
    public int getInt(String key) throws Exception {
        Object value = data.get(key);
        if (value == null) {
            throw new Exception("No key was found:" + key);
        }
        return Integer.parseInt(value.toString());
    }

    @Override
    public int getInt(String key, int defaultValue) {
        try {
            return getInt(key);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    @Override
    public long getLong(String key) throws Exception {
        Object value = data.get(key);
        if (value == null) {
            throw new Exception("No key was found:" + key);
        }
        return Long.parseLong(value.toString());
    }

    @Override
    public long getLong(String key, long defaultValue) {
        try {
            return getLong(key);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    @Override
    public double getDouble(String key) throws Exception {
        Object value = data.get(key);
        if (value == null) {
            throw new Exception("No key was found:" + key);
        }
        return Double.parseDouble(value.toString());
    }

    @Override
    public double getDouble(String key, double defaultValue) {
        try {
            return getDouble(key);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    @Override
    public void printConfig() {
        try {
            logger.error("------------------------------");
            logger.info("Job will run with arguments:");
            logger.info("---");
            data.forEach((key, value) -> {
                if (key != null && key.contains("EXECUTION_DATE"))
                    logger.error(String.format("%s=%s", key, value));
                else
                    logger.info(String.format("%s=%s", key, value));

            });

            logger.error(String.format("Timezone: %s", TimeZone.getDefault().getDisplayName()));
            logger.error(String.format("Current time: %d - %s",
                    System.currentTimeMillis(),
                    TimestampUtils.format(System.currentTimeMillis(), Option.apply("yyyy-MM-dd HH:mm:ss"), Option.apply(null))
            ));
            logger.error(String.format("Report will be calculated for: %d - %s",
                    getDailyReportTime(),
                    TimestampUtils.format(getDailyReportTime(), Option.apply("yyyy-MM-dd HH:mm:ss"), Option.apply(null))
            ));

            logger.error("------------------------------");
        } catch (Exception ignored) {

        }
    }


    @Override
    public Config cloneWith(Map<String, Object> data) {

        Map<String, Object> newData = new HashMap<>(this.data);

        if (data != null) {
            newData.putAll(data);
        }

        return new DefaultConfig(newData);
    }
}
