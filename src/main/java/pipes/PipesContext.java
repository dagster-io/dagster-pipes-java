package pipes;

import generated.Method;
import pipes.utils.PipesUtils;
import pipes.writers.PipesMessageWriter;
import pipes.writers.PipesMessageWriterChannel;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class PipesContext {

    private static PipesContext instance = null;
    private PipesContextData data = null;
    private PipesMessageWriterChannel messageChannel = null;
    private final Set<String> materializedAssets;
    private boolean closed;
    private final Logger logger;
    private Exception exception;

    public PipesContext(
        PipesParamsLoader paramsLoader,
        PipesContextLoader contextLoader,
        PipesMessageWriter messageWriter
    ) throws DagsterPipesException, IOException {
        Optional<Map<String, Object>> contextParams = paramsLoader.loadContextParams();
        Optional<Map<String, Object>> messageParams = paramsLoader.loadMessagesParams();
        if (contextParams.isPresent() && messageParams.isPresent()) {
            this.data = contextLoader.loadContext(contextParams.get());
            this.messageChannel = messageWriter.open(messageParams.get());
            Map<String, Object> openedPayload = messageWriter.getOpenedPayload();
            this.messageChannel.writeMessage(PipesUtils.makeMessage(Method.OPENED, openedPayload));
        }
        // ToDO:: fix logger
        this.logger = Logger.getLogger(PipesContext.class.getName());
        this.materializedAssets = new HashSet<>();
        this.closed = false;
        this.exception = null;
    }

    public void reportException(Exception exception) {
        this.exception = exception;
    }

    public void close() throws IOException {
        if (!closed) {
            Map<String, Object> payload = new HashMap<>();
            if (exception != null) {
                payload.put("exception", exception.getMessage());
            }
            this.messageChannel.writeMessage(PipesUtils.makeMessage(Method.CLOSED, payload));
            closed = true;
        }
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

//    public String getAssetKey() {
//        List<String> assetKeys = getDefinedAssetProperty(data.getAssetKeys(), "asset_key");
//        assertSingleAsset();
//        return assetKeys.get(0);
//    }
//
//    public List<String> getAssetKeys() {
//        return getDefinedAssetProperty("asset_keys", "asset_keys");
//    }
//
//    public Optional<PipesDataProvenance> getProvenance() {
//        Map<String, PipesDataProvenance> provenanceByAssetKey =
//            getDefinedAssetProperty("provenance_by_asset_key", "provenance");
//        assertSingleAsset();
//        return Optional.ofNullable(provenanceByAssetKey.values().iterator().next());
//    }
//
//    public Map<String, PipesDataProvenance> getProvenanceByAssetKey() {
//        return getDefinedAssetProperty("provenance_by_asset_key", "provenance_by_asset_key");
//    }
//
//    public Optional<String> getCodeVersion() {
//        Map<String, String> codeVersionByAssetKey = getDefinedAssetProperty(
//            "code_version_by_asset_key",
//            "code_version"
//        );
//        assertSingleAsset();
//        return Optional.ofNullable(codeVersionByAssetKey.values().iterator().next());
//    }
//
//    public Map<String, String> getCodeVersionByAssetKey() {
//        return getDefinedAssetProperty("code_version_by_asset_key", "code_version_by_asset_key");
//    }
//
//    public boolean isPartitionStep() {
//        return data.getPartitionKeyRange() != null;
//    }
//
//    public String getPartitionKey() {
//        return getDefinedPartitionProperty("partition_key", "partition_key");
//    }
//
//    public PipesPartitionKeyRange getPartitionKeyRange() {
//        return getDefinedPartitionProperty("partition_key_range", "partition_key_range");
//    }
//
//    public PipesTimeWindow getPartitionTimeWindow() {
//        getDefinedPartitionProperty("partition_key_range", "partition_time_window");
//        return data.getPartitionTimeWindow();
//    }
//
//    public String getRunId() {
//        return data.getRunId();
//    }
//
//    public String getJobName() {
//        return data.getJobName();
//    }
//
//    public int getRetryNumber() {
//        return data.getRetryNumber();
//    }
//
//    public Object getExtra(String key) {
//        Map<String, Object> extras = data.getExtras();
//        if (!extras.containsKey(key)) {
//            throw new IllegalArgumentException("Extra " + key + " is not defined.");
//        }
//        return extras.get(key);
//    }
//
//    public Map<String, Object> getExtras() {
//        return data.getExtras();
//    }
//
//    public void reportAssetMaterialization(
//        Map<String, Object> metadata,
//        String dataVersion,
//        String assetKey
//    ) throws IOException {
//        assetKey = resolveOptionallyPassedAssetKey(assetKey);
//        if (materializedAssets.contains(assetKey)) {
//            throw new IllegalStateException(
//                "Asset key `" + assetKey + "` has already been materialized, cannot report additional data."
//            );
//        }
//        Map<String, Object> map = new HashMap<>();
//        map.put("asset_key", assetKey);
//        map.put("data_version", dataVersion);
//        map.put("metadata", metadata);
//        sendMessage("report_asset_materialization", map);
//        materializedAssets.add(assetKey);
//    }
//
//    public void reportAssetCheck(
//        String checkName,
//        boolean passed,
//        String severity,
//        Map<String, Object> metadata,
//        String assetKey
//    ) throws IOException {
//        assetKey = resolveOptionallyPassedAssetKey(assetKey);
//        Map<String, Object> map = new HashMap<>();
//        map.put("asset_key", assetKey);
//        map.put("check_name", checkName);
//        map.put("passed", passed);
//        map.put("metadata", metadata);
//        map.put("severity", severity);
//        sendMessage("report_asset_check", map);
//    }
//
//    public void reportCustomMessage(Object payload) throws IOException {
//        Map<String, Object> map = new HashMap<>();
//        map.put("payload", payload);
//        sendMessage("report_custom_message", map);
//    }
//
//    public Logger getLogger() {
//        return logger;
//    }
//
//    @Override
//    public void close() throws IOException {
//        if (!closed) {
//            sendMessage("closed", new HashMap<>());
//            closeResources();
//            closed = true;
//        }
//    }
//
//    private void sendMessage(String method, Map<String, Object> params) throws IOException {
//        if (closed) {
//            throw new IllegalStateException("Cannot send message after pipes context is closed.");
//        }
//        messageChannel.writeMessage(PipesUtils.makeMessage(method, params));
//    }
//
//    private void closeResources() {
//        while (!ioStack.isEmpty()) {
//            try {
//                ioStack.pop().close();
//            } catch (Exception e) {
//                throw new RuntimeException("Error closing resources", e);
//            }
//        }
//    }
//
//    private <T> T getDefinedAssetProperty(String key, String errorContext) {
//        return assertDefinedProperty(data, key, errorContext);
//    }
//
//    private <T> T getDefinedPartitionProperty(String key, String errorContext) {
//        return assertDefinedProperty(data, key, errorContext);
//    }
//
//    private <T> T assertDefinedProperty(
//        PipesContextData pipesContextData,
//        String key,
//        String errorContext
//    ) {
//        if (!pipesContextData.containsKey(key)) {
//            throw new IllegalArgumentException(errorContext + " is not defined.");
//        }
//        return (T) map.get(key);
//    }
//
//    private void assertSingleAsset() {
//        List<String> assetKeys = data.getAssetKeys();
//        if (assetKeys == null || assetKeys.size() != 1) {
//            throw new IllegalStateException("Expected a single asset.");
//        }
//    }
//
//    private String resolveOptionallyPassedAssetKey(String assetKey) {
//        if (assetKey == null) {
//            List<String> assetKeys = data.getAssetKeys();
//            if (assetKeys.size() != 1) {
//                throw new IllegalStateException("Multiple assets in scope, specify the asset key.");
//            }
//            return assetKeys.get(0);
//        }
//        return assetKey;
//    }
}