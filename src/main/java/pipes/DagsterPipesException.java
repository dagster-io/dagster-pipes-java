package pipes;

public class DagsterPipesException extends Exception {

    public DagsterPipesException(String message) {
        super(message);
    }

    public DagsterPipesException(String message, Throwable cause) {
        super(message, cause);
    }

}
