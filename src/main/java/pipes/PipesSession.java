package pipes;

import pipes.loaders.PipesContextLoader;
import pipes.loaders.PipesDefaultContextLoader;
import pipes.loaders.PipesEnvVarParamsLoader;
import pipes.loaders.PipesParamsLoader;
import pipes.writers.PipesDefaultMessageWriter;
import pipes.writers.PipesMessageWriter;

import java.io.IOException;
import java.util.logging.Logger;

import static org.mockito.Mockito.mock;

public class PipesSession implements AutoCloseable {

    private final PipesContext context;

    private static final Logger logger = Logger.getLogger(PipesSession.class.getName());

    PipesSession(PipesContext context) {
        this.context = context;
    }

    public PipesContext openDagsterPipes(
        PipesParamsLoader paramsLoader,
        PipesContextLoader contextLoader,
        PipesMessageWriter messageWriter
    ) throws DagsterPipesException, IOException {
        if (PipesContext.isInitialized()) {
            return PipesContext.get();
        }

        if (paramsLoader == null) {
            paramsLoader = new PipesEnvVarParamsLoader();
        }

        PipesContext pipesContext;
        if (paramsLoader.isDagsterPipesProcess()) {
            if (contextLoader == null) {
                contextLoader = new PipesDefaultContextLoader();
            }
            if (messageWriter == null) {
                messageWriter = new PipesDefaultMessageWriter();
            }
            pipesContext = new PipesContext(
                paramsLoader, contextLoader, messageWriter
            );
        } else {
            emitOrchestrationInactiveWarning();
            // TODO:: figure out how to remove Mockito
            pipesContext = mock(PipesContext.class);
        }
        PipesContext.set(pipesContext);
        return pipesContext;
    }

    private static void emitOrchestrationInactiveWarning() {
        logger.warning(
            "This process was not launched by a Dagster orchestration process. All calls to the " +
            "`dagster-pipes` context or attempts to initialize " +
            "`dagster-pipes` abstractions are no-ops."
        );
    }

    public PipesContext getContext() {
        return context;
    }

    @Override
    public void close() throws Exception {
        this.context.close();
    }
}