package pipes.writers;

import pipes.DagsterPipesException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class PipesFileMessageWriterChannel extends PipesMessageWriter<PipesMessageWriterChannel> implements PipesMessageWriterChannel {

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
        } catch (IOException ioException) {
            throw new DagsterPipesException("Failed to write to the file: " + path, ioException);
        }
    }

    public String getPath() {
        return path;
    }

    @Override
    public PipesMessageWriterChannel open(Map<String, Object> params) {
        return this;
    }
}
