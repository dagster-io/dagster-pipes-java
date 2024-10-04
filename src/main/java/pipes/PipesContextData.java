package pipes;

import java.util.List;
import java.util.Map;

public class PipesContextData {

    private List<String> assetKeys;  // Can be null if not provided
    private Map<String, String> codeVersionByAssetKey;  // Can be null
    private Map<String, PipesDataProvenance> provenanceByAssetKey;  // Can be null
    private String partitionKey;  // Can be null
    private PipesPartitionKeyRange partitionKeyRange;  // Can be null
    private PipesTimeWindow partitionTimeWindow;  // Can be null
    private String runId;  // Required
    private String jobName;  // Can be null
    private int retryNumber;  // Required
    private Map<String, Object> extras;  // Required

    // Constructor
    public PipesContextData(List<String> assetKeys,
        Map<String, String> codeVersionByAssetKey,
        Map<String, PipesDataProvenance> provenanceByAssetKey,
        String partitionKey,
        PipesPartitionKeyRange partitionKeyRange,
        PipesTimeWindow partitionTimeWindow,
        String runId,
        String jobName,
        int retryNumber,
        Map<String, Object> extras
    ) {
        this.assetKeys = assetKeys;
        this.codeVersionByAssetKey = codeVersionByAssetKey;
        this.provenanceByAssetKey = provenanceByAssetKey;
        this.partitionKey = partitionKey;
        this.partitionKeyRange = partitionKeyRange;
        this.partitionTimeWindow = partitionTimeWindow;
        this.runId = runId;
        this.jobName = jobName;
        this.retryNumber = retryNumber;
        this.extras = extras;
    }

    // Getters and Setters for each field
    public List<String> getAssetKeys() {
        return assetKeys;
    }

    public void setAssetKeys(List<String> assetKeys) {
        this.assetKeys = assetKeys;
    }

    public Map<String, String> getCodeVersionByAssetKey() {
        return codeVersionByAssetKey;
    }

    public void setCodeVersionByAssetKey(Map<String, String> codeVersionByAssetKey) {
        this.codeVersionByAssetKey = codeVersionByAssetKey;
    }

    public Map<String, PipesDataProvenance> getProvenanceByAssetKey() {
        return provenanceByAssetKey;
    }

    public void setProvenanceByAssetKey(Map<String, PipesDataProvenance> provenanceByAssetKey) {
        this.provenanceByAssetKey = provenanceByAssetKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public PipesPartitionKeyRange getPartitionKeyRange() {
        return partitionKeyRange;
    }

    public void setPartitionKeyRange(PipesPartitionKeyRange partitionKeyRange) {
        this.partitionKeyRange = partitionKeyRange;
    }

    public PipesTimeWindow getPartitionTimeWindow() {
        return partitionTimeWindow;
    }

    public void setPartitionTimeWindow(PipesTimeWindow partitionTimeWindow) {
        this.partitionTimeWindow = partitionTimeWindow;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public int getRetryNumber() {
        return retryNumber;
    }

    public void setRetryNumber(int retryNumber) {
        this.retryNumber = retryNumber;
    }

    public Map<String, Object> getExtras() {
        return extras;
    }

    public void setExtras(Map<String, Object> extras) {
        this.extras = extras;
    }
}