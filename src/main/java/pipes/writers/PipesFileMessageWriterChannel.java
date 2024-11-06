package pipes.writers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class PipesFileMessageWriterChannel implements PipesMessageWriterChannel {

    private final String path;

    public PipesFileMessageWriterChannel(String path) {
        this.path = path;
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
                System.out.println(file.getAbsolutePath());
                fileWriter.write(message.toString() + System.lineSeparator());
                fileWriter.flush();
            }
        } catch (IOException e) {
            throw new IOException("Failed to write message to file: " + path, e);
        }
    }

    public String getPath() {
        return path;
    }
}
