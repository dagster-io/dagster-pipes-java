package pipes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

@Disabled
public class ContextDataTest {

    private final PipesContextData contextData;
    private Map<String, String> extras;
    private String jobName;

    ContextDataTest(Map<String, String> input) throws DagsterPipesException {
        this.contextData = DataLoader.getData(input);
    }

    void setExtras(Map<String, String> extras) {
        this.extras = extras;
    }

    void setJobName(String jobName) {
       this.jobName = jobName;
    }

    @Test
    public void testExtras() {
        Set<Map.Entry<String, String>> sanitizedExtras = TestUtils
            .sanitizeMapEntries(this.extras);

        Assertions.assertTrue(
            contextData.getExtras().entrySet().containsAll(sanitizedExtras),
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
}