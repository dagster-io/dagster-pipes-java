package pipes;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileWriter;
import java.io.IOException;

public class PipesFileMessageWriterChannel implements PipesMessageWriterChannel {

    private final String path;
    private final ObjectMapper objectMapper;

    public PipesFileMessageWriterChannel(String path) {
        this.path = path;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void writeMessage(PipesMessage message) throws IOException {
        try (FileWriter fileWriter = new FileWriter(this.path, true)) {
            String jsonMessage = objectMapper.writeValueAsString(message);
            fileWriter.write(jsonMessage + System.lineSeparator());
        } catch (IOException e) {
            throw new IOException("Failed to write message to file: " + path, e);
        }
    }
}
