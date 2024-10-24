package pipes;

public class PipesSession implements AutoCloseable {

    private final PipesContext context;

    PipesSession(PipesContext context) {
        this.context = context;
    }

    @Override
    public void close() throws Exception {
        this.context.close();
    }
}