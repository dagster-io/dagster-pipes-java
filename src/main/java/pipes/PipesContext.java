package pipes;

import pipes.constants.Method;
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
        if (!this.closed) {
            Map<String, Object> payload = new HashMap<>();
            if (this.exception != null) {
                payload.put("exception", this.exception.getMessage());
            }
            this.messageChannel.writeMessage(PipesUtils.makeMessage(Method.CLOSED, payload));
            this.closed = true;
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

    public void writeMessage(
        Method method,
        Map<String, Object> params
    ) throws DagsterPipesException, IOException {
        if (this.closed) {
            throw new DagsterPipesException("Cannot send message after pipes context is closed.");
        }
        this.messageChannel.writeMessage(PipesUtils.makeMessage(method, params));
    }

    public boolean isClosed() {
        return this.closed;
    }

    public boolean isAssetStep() {
        return this.data.getAssetKeys() != null;
    }

    public String getAssetKey() throws DagsterPipesException {
        List<String> assetKeys = getAssetKeys();
        assertSingleAsset(assetKeys, "Asset key");
        return assetKeys.get(0);
    }

    public List<String> getAssetKeys() throws DagsterPipesException {
        List<String> assetKeys = this.data.getAssetKeys();
        assertPresence(assetKeys, "Asset keys");
        return assetKeys;
    }

    public PipesDataProvenance getProvenance() throws DagsterPipesException {
        Map<String, PipesDataProvenance> provenanceByAssetKey = getProvenanceByAssetKey();
        assertSingleAsset(provenanceByAssetKey, "Provenance");
        return provenanceByAssetKey.values().iterator().next();
    }

    public Map<String, PipesDataProvenance> getProvenanceByAssetKey() throws DagsterPipesException {
        Map<String, PipesDataProvenance> provenanceByAssetKey = this.data.getProvenanceByAssetKey();
        assertPresence(provenanceByAssetKey, "Provenance by asset key");
        return provenanceByAssetKey;
    }

    public String getCodeVersion() throws DagsterPipesException {
        Map<String, String> codeVersionByAssetKey = getCodeVersionByAssetKey();
        assertSingleAsset(codeVersionByAssetKey, "Code version");
        return codeVersionByAssetKey.values().iterator().next();
    }

    public Map<String, String> getCodeVersionByAssetKey() throws DagsterPipesException {
        Map<String, String> codeVersionByAssetKey = this.data.getCodeVersionByAssetKey();
        assertPresence(codeVersionByAssetKey, "Code version by asset key");
        return codeVersionByAssetKey;
    }

    public boolean isPartitionStep() {
        return this.data.getPartitionKeyRange() != null;
    }


    public String getPartitionKey() throws DagsterPipesException {
        String partitionKey = this.data.getPartitionKey();
        assertPresence(partitionKey, "Partition key");
        return partitionKey;
    }

    public PartitionKeyRange getPartitionKeyRange() throws DagsterPipesException {
        PartitionKeyRange partitionKeyRange = this.data.getPartitionKeyRange();
        assertPresence(partitionKeyRange, "Partition key range");
        return partitionKeyRange;
    }

    public PartitionTimeWindow getPartitionTimeWindow() throws DagsterPipesException {
        PartitionTimeWindow partitionTimeWindow = this.data.getPartitionTimeWindow();
        assertPresence(partitionTimeWindow, "Partition time window");
        return partitionTimeWindow;
    }

    public String getRunId() {
        return this.data.getRunId();
    }

    public String getJobName() {
        return this.data.getJobName();
    }

    public int getRetryNumber() {
        return this.data.getRetryNumber();
    }

    public Object getExtra(String key) throws DagsterPipesException {
        Map<String, Object> extras = this.data.getExtras();
        if (!extras.containsKey(key)) {
            throw new DagsterPipesException(
                String.format("Extra %s is undefined. Extras must be provided by user.", key)
            );
        }
        return extras.get(key);
    }

    public Map<String, Object> getExtras() {
        return this.data.getExtras();
    }

    private void assertSingleAsset(Collection<?> collection, String name) throws DagsterPipesException {
        if (collection.size() != 1) {
            throw new DagsterPipesException(
                String.format("%s is undefined. Current step targets multiple assets.", name)
            );
        }
    }

    private void assertSingleAsset(Map<?, ?> map, String name) throws DagsterPipesException {
        if (map.size() != 1) {
            throw new DagsterPipesException(
                String.format("%s is undefined. Current step targets multiple assets.", name)
            );
        }
    }

    private void assertPresence(Object object, String name) throws DagsterPipesException {
        if (object == null) {
            throw new DagsterPipesException(
                String.format("%s is undefined. Current step does not target an asset.", name)
            );
        }
        if (object instanceof Collection<?> && ((Collection<?>) object).isEmpty()) {
            throw new DagsterPipesException(
                String.format("%s is empty. Current step does not target an asset.", name)
            );
        }
    }


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