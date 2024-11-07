package pipes;

public interface ThrowingConsumer {
    void run(PipesSession session) throws Exception;
}
