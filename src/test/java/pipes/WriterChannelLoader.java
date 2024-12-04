//package pipes;
//
//import pipes.loaders.PipesEnvVarParamsLoader;
//import pipes.loaders.PipesMappingParamsLoader;
//import pipes.writers.PipesDefaultMessageWriter;
//import pipes.writers.PipesMessageWriterChannel;
//
//import java.util.Map;
//import java.util.Optional;
//
//public class WriterChannelLoader {
//
//    static PipesMessageWriterChannel getWriter() throws DagsterPipesException {
//        PipesMappingParamsLoader paramsLoader = new PipesEnvVarParamsLoader();
//        PipesDefaultMessageWriter defaultMessageWriter = new PipesDefaultMessageWriter();
//        Optional<Map<String, Object>> params = paramsLoader.loadMessagesParams();
//        if (params.isPresent()) {
//            return defaultMessageWriter.open(params.get());
//        } else throw new DagsterPipesException("Can't load PipesMessageWriterChannel");
//    }
//
//    static PipesMessageWriterChannel getWriter(Map<String, String> input) throws DagsterPipesException {
//        if (input.isEmpty()) {
//            return getWriter();
//        }
//        PipesMappingParamsLoader paramsLoader = new PipesMappingParamsLoader(input);
//        PipesDefaultMessageWriter defaultMessageWriter = new PipesDefaultMessageWriter();
//        Optional<Map<String, Object>> params = paramsLoader.loadMessagesParams();
//        if (params.isPresent()) {
//            return defaultMessageWriter.open(params.get());
//        } else throw new DagsterPipesException("Can't load PipesMessageWriterChannel");
//    }
//}