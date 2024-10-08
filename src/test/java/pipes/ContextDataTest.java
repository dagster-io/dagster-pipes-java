package pipes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

@Disabled
public class ContextDataTest {

    private PipesContextData contextData;
    private Map<String, Object> extras;
    private String jobName;

    ContextDataTest(PipesContextData pipesContextData) throws DagsterPipesException {
        this.contextData = pipesContextData;
    }

    void setExtras(Map<String, Object> extras) {
        this.extras = extras;
    }

    void setJobName(String jobName) {
       this.jobName = jobName;
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
}