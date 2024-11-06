package pipes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import pipes.data.PipesContextData;
import pipes.loaders.PipesContextLoader;
import pipes.loaders.PipesDefaultContextLoader;
import pipes.loaders.PipesEnvVarParamsLoader;
import pipes.loaders.PipesParamsLoader;
import pipes.writers.*;
import types.Type;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Disabled
public class PipesTests {

    private Map<String, String> input;
    private PipesContextData contextData;
    private Map<String, Object> extras;
    private String jobName;
    private Object payload;

    private Map<String, PipesMetadata> metadata = null;

    //Related to reportAssetMaterialization
    private boolean materialization = false;
    private String dataVersion;
    private String materializationAssetKey;

    //Related to reportAssetCheck
    private boolean check = false;
    private String checkName;
    private boolean passed;
    private String checkAssetKey;

    void setInput(Map<String, String> input) {
        this.input = input;
    }

    void setExtras(Map<String, Object> extras) {
        this.extras = extras;
    }

    void setJobName(String jobName) {
        this.jobName = jobName;
    }

    void setContextData() throws DagsterPipesException {
        this.contextData = DataLoader.getData(input);
    }

    void setPayload(Object payload) {
        this.payload = payload;
    }

    void setMaterialization(String dataVersion, String assetKey) {
        this.materialization = true;
        this.dataVersion = dataVersion;
        this.materializationAssetKey = assetKey;
    }

    void setCheck(String checkName, boolean passed, String assetKey) {
        this.check = true;
        this.checkName = checkName;
        this.passed = passed;
        this.checkAssetKey = assetKey;
    }

    @Test
    public void testExtras() {
        Assertions.assertTrue(
            contextData.getExtras().entrySet().containsAll(this.extras.entrySet()),
            "Extras does not contain all provided entries."
        );
        System.out.println("Extras are correct.");
    }

    @Test
    public void testJobName() {
        Assertions.assertEquals(
            this.jobName,
            contextData.getJobName(),
            "JobName is incorrect."
        );
        System.out.println("JobName is correct.");
    }

    @Test
    public void fullTest() throws DagsterPipesException, IOException {
        PipesParamsLoader paramsLoader = new PipesEnvVarParamsLoader();
        PipesContextLoader contextLoader = new PipesDefaultContextLoader();
        PipesMessageWriter messageWriter = new PipesDefaultMessageWriter();
        PipesContext pipesContext = new PipesContext(paramsLoader, contextLoader, messageWriter);
        try (PipesSession session = new PipesSession(pipesContext)) {
            session.openDagsterPipes(paramsLoader, contextLoader, messageWriter);
            System.out.println("Opened dagster pipes with set params.");
            if (this.payload != null) {
                session.getContext().reportCustomMessage(this.payload);
                System.out.println("Payload reported with custom message.");
            }

            if (this.materialization) {
                buildTestMetadata();
                session.getContext().reportAssetMaterialization(
                    this.metadata, this.dataVersion, this.materializationAssetKey
                );
            }
            if (this.check) {
                buildTestMetadata();
                session.getContext().reportAssetCheck(
                    this.checkName, this.passed, this.metadata, this.checkAssetKey
                );
            }
            System.out.println("Finished try session");
        } catch (Exception exception) {
            pipesContext.reportException(exception);
        }
    }

    public Map<String, PipesMetadata> buildTestMetadata() {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
            this.metadata.put("float", new PipesMetadata(0.1, Type.FLOAT));
            this.metadata.put("int", new PipesMetadata(1, Type.INT));
            this.metadata.put("text", new PipesMetadata("hello", Type.TEXT));
            this.metadata.put("notebook", new PipesMetadata("notebook.ipynb", Type.NOTEBOOK));
            this.metadata.put("path", new PipesMetadata("/dev/null", Type.PATH));
            this.metadata.put("md", new PipesMetadata("**markdown**", Type.MD));
            this.metadata.put("bool_true", new PipesMetadata(true, Type.BOOL));
            this.metadata.put("bool_false", new PipesMetadata(false, Type.BOOL));
            this.metadata.put("asset", new PipesMetadata("foo/bar", Type.ASSET));
            this.metadata.put(
                "dagster_run",
                new PipesMetadata("db892d7f-0031-4747-973d-22e8b9095d9d", Type.DAGSTER_RUN)
            );
            this.metadata.put("null", new PipesMetadata(null, Type.NULL));
            this.metadata.put("url", new PipesMetadata("https://dagster.io", Type.URL));
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("foo", "bar");
            jsonMap.put("baz", 1);
            jsonMap.put("qux", new int[]{1, 2, 3});
            Map<String, Integer> inner = new HashMap<>();
            inner.put("a", 1);
            inner.put("b", 2);
            jsonMap.put("quux", inner);
            jsonMap.put("corge", null);
            this.metadata.put("json", new PipesMetadata(jsonMap, Type.JSON));
        }
        return this.metadata;
    }
}
