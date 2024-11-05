package pipes.writers;

import java.util.Map;

import static pipes.data.PipesConstants.PIPES_PROTOCOL_VERSION_FIELD;

public class PipesMessage {

    private String dagsterPipesVersion;
    private String method;
    private Map<String, Object> params;

    public PipesMessage(String dagsterPipesVersion, String method, Map<String, Object> params) {
        this.dagsterPipesVersion = dagsterPipesVersion;
        this.method = method;
        this.params = params;
    }

    public String getDagsterPipesVersion() {
        return dagsterPipesVersion;
    }

    public void setDagsterPipesVersion(String dagsterPipesVersion) {
        this.dagsterPipesVersion = dagsterPipesVersion;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public boolean hasParams() {
        return this.params != null;
    }

//    @Override
//    public String toString() {
//        return String.format(
//            "PipesMessage{%s='%s\\, method=%s\\, params=%s\\}",
//            PIPES_PROTOCOL_VERSION_FIELD, dagsterPipesVersion, method, params
//        );
//    }
}
