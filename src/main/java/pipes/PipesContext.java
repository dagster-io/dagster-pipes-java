package pipes;

import pipes.utils.PipesUtils;
import pipes.writers.PipesMessageWriter;
import pipes.writers.PipesMessageWriterChannel;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class PipesContext implements Closeable {

    private static PipesContext instance = null;
    private final Deque<AutoCloseable> ioStack = new ArrayDeque<>();
    private final PipesContextData data;
    private final PipesMessageWriterChannel messageChannel;
    private final Set<String> materializedAssets = new HashSet<>();
    private boolean closed = false;
    private final Logger logger;

    private PipesContext(
        PipesParamsLoader paramsLoader,
        PipesContextLoader contextLoader,
        PipesMessageWriter messageWriter
    ) throws Exception {
        Map<String, Object> contextParams = paramsLoader.loadContextParams();
        Map<String, Object> messageParams = paramsLoader.loadMessagesParams();
        this.data = contextLoader.loadContext(contextParams);
        this.messageChannel = messageWriter.open(messageParams);
        Map<String, Object> openedPayload = messageWriter.getOpenedPayload();
        this.messageChannel.writeMessage(PipesUtils.makeMessage("opened", openedPayload));
        this.logger = Logger.getLogger(PipesContext.class.getName());
    }

    public static boolean isInitialized() {
        return instance != null;
    }

    public static void set(PipesContext context) {
        instance = context;
    }

    public static PipesContext get() {
        if (instance == null) {
            throw new IllegalStateException(
                "PipesContext has not been initialized. You must call openDagsterPipes()."
            );
        }
        return instance;
    }

    public boolean isAssetStep() {
        return data.getAssetKeys() != null;
    }

    public String getAssetKey() {
        List<String> assetKeys = getDefinedAssetProperty("asset_keys", "asset_key");
        assertSingleAsset();
        return assetKeys.get(0);
    }

    public List<String> getAssetKeys() {
        return getDefinedAssetProperty("asset_keys", "asset_keys");
    }

    public Optional<PipesDataProvenance> getProvenance() {
        Map<String, PipesDataProvenance> provenanceByAssetKey =
            getDefinedAssetProperty("provenance_by_asset_key", "provenance");
        assertSingleAsset();
        return Optional.ofNullable(provenanceByAssetKey.values().iterator().next());
    }

    public Map<String, PipesDataProvenance> getProvenanceByAssetKey() {
        return getDefinedAssetProperty("provenance_by_asset_key", "provenance_by_asset_key");
    }

    public Optional<String> getCodeVersion() {
        Map<String, String> codeVersionByAssetKey = getDefinedAssetProperty(
            "code_version_by_asset_key",
            "code_version"
        );
        assertSingleAsset();
        return Optional.ofNullable(codeVersionByAssetKey.values().iterator().next());
    }

    public Map<String, String> getCodeVersionByAssetKey() {
        return getDefinedAssetProperty("code_version_by_asset_key", "code_version_by_asset_key");
    }

    public boolean isPartitionStep() {
        return data.getPartitionKeyRange() != null;
    }

    public String getPartitionKey() {
        return getDefinedPartitionProperty("partition_key", "partition_key");
    }

    public PipesPartitionKeyRange getPartitionKeyRange() {
        return getDefinedPartitionProperty("partition_key_range", "partition_key_range");
    }

    public Optional<PipesTimeWindow> getPartitionTimeWindow() {
        getDefinedPartitionProperty("partition_key_range", "partition_time_window");
        return Optional.ofNullable((PipesTimeWindow) data.get("partition_time_window"));
    }

    public String getRunId() {
        return (String) data.get("run_id");
    }

    public Optional<String> getJobName() {
        return Optional.ofNullable((String) data.get("job_name"));
    }

    public int getRetryNumber() {
        return (int) data.get("retry_number");
    }

    public Object getExtra(String key) {
        Map<String, Object> extras = (Map<String, Object>) data.get("extras");
        if (!extras.containsKey(key)) {
            throw new IllegalArgumentException("Extra " + key + " is not defined.");
        }
        return extras.get(key);
    }

    public Map<String, Object> getExtras() {
        return (Map<String, Object>) data.get("extras");
    }

    // Asset Reporting
    public void reportAssetMaterialization(
            Map<String, Object> metadata,
            String dataVersion,
            String assetKey
    ) {
        assetKey = resolveOptionallyPassedAssetKey(assetKey);
        if (materializedAssets.contains(assetKey)) {
            throw new IllegalStateException(
                    "Asset key `" + assetKey + "` has already been materialized, cannot report additional data."
            );
        }
        Map<String, Object> map = new HashMap<>();
        map.put("asset_key", assetKey);
        map.put( "data_version", dataVersion);
        map.put( "metadata", metadata);
        sendMessage("report_asset_materialization", map);
        materializedAssets.add(assetKey);
    }

    public void reportAssetCheck(
            String checkName,
            boolean passed,
            String severity,
            Map<String, Object> metadata,
            String assetKey
    ) {
        assetKey = resolveOptionallyPassedAssetKey(assetKey);
        Map<String, Object> map = new HashMap<>();
        map.put("asset_key", assetKey);
        map.put("check_name", checkName);
        map.put("passed", passed);
        map.put("metadata", metadata);
        map.put("severity", severity);
        sendMessage("report_asset_check", map);
    }

    public void reportCustomMessage(Object payload) {
        Map<String, Object> map = new HashMap<>();
        map.put("payload", payload);
        sendMessage("report_custom_message", map);
    }

    public Logger getLogger() {
        return logger;
    }

    @Override
    public void close() {
        if (!closed) {
            sendMessage("closed", new HashMap<>());
            closeResources();
            closed = true;
        }
    }

    private void sendMessage(String method, Map<String, Object> params) throws IOException {
        if (closed) {
            throw new IllegalStateException("Cannot send message after pipes context is closed.");
        }
        messageChannel.writeMessage(PipesUtils.makeMessage(method, params));
    }

    private void closeResources() {
        while (!ioStack.isEmpty()) {
            try {
                ioStack.pop().close();
            } catch (Exception e) {
                throw new RuntimeException("Error closing resources", e);
            }
        }
    }

    private <T> T getDefinedAssetProperty(String key, String errorContext) {
        return assertDefinedProperty(data, key, errorContext);
    }

    private <T> T getDefinedPartitionProperty(String key, String errorContext) {
        return assertDefinedProperty(data, key, errorContext);
    }

    private <T> T assertDefinedProperty(Map<String, Object> map, String key, String errorContext) {
        if (!map.containsKey(key)) {
            throw new IllegalArgumentException(errorContext + " is not defined.");
        }
        return (T) map.get(key);
    }

    private void assertSingleAsset() {
        List<String> assetKeys = (List<String>) data.getAssetKeys();
        if (assetKeys == null || assetKeys.size() != 1) {
            throw new IllegalStateException("Expected a single asset.");
        }
    }

    private String resolveOptionallyPassedAssetKey(String assetKey) {
        if (assetKey == null) {
            List<String> assetKeys = data.getAssetKeys();
            if (assetKeys.size() != 1) {
                throw new IllegalStateException("Multiple assets in scope, specify the asset key.");
            }
            return assetKeys.get(0);
        }
        return assetKey;
    }
}