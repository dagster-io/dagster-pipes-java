package pipes.data;

import java.util.Map;

public class PipesDataProvenance {

    private String codeVersion;
    private Map<String, String> inputDataVersions;
    private boolean isUserProvided;

    public PipesDataProvenance(
        String codeVersion,
        Map<String, String> inputDataVersions,
        boolean isUserProvided
    ) {
        this.codeVersion = codeVersion;
        this.inputDataVersions = inputDataVersions;
        this.isUserProvided = isUserProvided;
    }

    public String getCodeVersion() {
        return codeVersion;
    }

    public void setCodeVersion(String codeVersion) {
        this.codeVersion = codeVersion;
    }

    public Map<String, String> getInputDataVersions() {
        return inputDataVersions;
    }

    public void setInputDataVersions(Map<String, String> inputDataVersions) {
        this.inputDataVersions = inputDataVersions;
    }

    public boolean isUserProvided() {
        return isUserProvided;
    }

    public void setUserProvided(boolean userProvided) {
        isUserProvided = userProvided;
    }
}