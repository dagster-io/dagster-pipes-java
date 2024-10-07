package pipes;

import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws DagsterPipesException {
        Map<String, String> input = new HashMap<>();
        input.put("DAGSTER_PIPES_CONTEXT","eJyNj11OwzAQhK8S9rmRatSmPyfgDgit1vUWXBw7WjtJoyp3rx0QIMQDjzvzjWbnBoYSwbG6AcXICd95ivl8hgsNhIsGL6sKTsEwDizRBo96wi96yf6Aj5XvnZtzpJMwsCd/4v8GSJJNpeADK/pvGYX8K/9lJtsyjtabMH7b0nu0Jt9AtN1ptT/Uj+vDtt4YxbVuGlWfm92ZzEYZtVeQE5eg0VNbKgCRuzduWch9/l9cLJhwkgl932qWjK6zxNckFJd51nd9WQZP7FxYVWMQZx5gnuc7Tcx9+A==");
        input.put("DAGSTER_PIPES_MESSAGES","eJyrVipILMlQslJQ0i/JLQDhrGQji9IMoxT93NTi4sT01GKlWgDo0wy5");

        PipesMappingParamsLoader paramsLoader = new PipesMappingParamsLoader(input);
        PipesDefaultContextLoader contextLoader = new PipesDefaultContextLoader();
        contextLoader.loadContext(paramsLoader.loadContextParams());

        System.out.println("debug");
    }
}
