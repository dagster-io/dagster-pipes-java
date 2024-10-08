package pipes;

public class PipesEnvVarParamsLoader extends PipesMappingParamsLoader {

    public PipesEnvVarParamsLoader() {
        super(System.getenv());
    }
}
