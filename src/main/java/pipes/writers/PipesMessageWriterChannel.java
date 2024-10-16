package pipes.writers;

import java.io.IOException;

public interface PipesMessageWriterChannel {

    void writeMessage(PipesMessage message) throws IOException;

}
