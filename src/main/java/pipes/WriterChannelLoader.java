package pipes;

import pipes.writers.PipesDefaultMessageWriter;
import pipes.writers.PipesMessageWriterChannel;

import java.util.Map;

public class WriterChannelLoader {

    public static PipesMessageWriterChannel getWriter() throws DagsterPipesException {
        PipesMappingParamsLoader paramsLoader = new PipesEnvVarParamsLoader();
        PipesDefaultMessageWriter defaultMessageWriter = new PipesDefaultMessageWriter();
        return defaultMessageWriter.open(paramsLoader.loadMessagesParams());
    }

    public static PipesMessageWriterChannel getWriter(
        Map<String, String> input
    ) throws DagsterPipesException {
        PipesMappingParamsLoader paramsLoader = new PipesMappingParamsLoader(input);
        PipesDefaultMessageWriter defaultMessageWriter = new PipesDefaultMessageWriter();
        return defaultMessageWriter.open(paramsLoader.loadMessagesParams());
    }
}
