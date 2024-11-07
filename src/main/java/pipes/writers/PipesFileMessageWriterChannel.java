package pipes.writers;

import pipes.DagsterPipesException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class PipesFileMessageWriterChannel implements PipesMessageWriterChannel {

    private final String path;

    public PipesFileMessageWriterChannel(String path) {
        this.path = path;
    }

    @Override
    public void writeMessage(PipesMessage message) throws DagsterPipesException {
        File file = new File(this.path);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            if (!parentDir.mkdirs()) {
                throw new DagsterPipesException("Failed to create directories for file: " + path);
            }
        }

        try (FileWriter fileWriter = new FileWriter(file, true)) {
            System.out.println(file.getAbsolutePath());
            fileWriter.write(message.toString() + System.lineSeparator());
            fileWriter.flush();
        }
    }

    public String getPath() {
        return path;
    }
}
