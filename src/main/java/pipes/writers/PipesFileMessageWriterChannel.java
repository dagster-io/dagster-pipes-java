package pipes.writers;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
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
        File file = new File(this.path);
        try {
            File parentDir = file.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                if (!parentDir.mkdirs()) {
                    throw new IOException("Failed to create directories for file: " + path);
                }
            }

            try (FileWriter fileWriter = new FileWriter(file, true)) {
                String jsonMessage = objectMapper.writeValueAsString(message);
                fileWriter.write(jsonMessage + System.lineSeparator());
            }
        } catch (IOException e) {
            throw new IOException("Failed to write message to file: " + path, e);
        }
    }

    public String getPath() {
        return path;
    }
}
