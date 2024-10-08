package pipes;

public enum PipesVariables {
    CONTEXT_ENV_VAR("DAGSTER_PIPES_CONTEXT"),
    MESSAGES_ENV_VAR("DAGSTER_PIPES_MESSAGES");

    public final String name;

    PipesVariables(final String name) {
        this.name = name;
    }
}