package pipes;

interface ThrowingConsumer {
    void run(PipesSession session) throws Exception;
}
