package pipes.writers;

import pipes.DagsterPipesException;

public interface PipesMessageWriterChannel {

    void writeMessage(PipesMessage message) throws DagsterPipesException;

}
