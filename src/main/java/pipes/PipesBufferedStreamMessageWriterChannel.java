package pipes;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class PipesBufferedStreamMessageWriterChannel implements PipesMessageWriterChannel {

    private final List<PipesMessage> buffer;
    private final BufferedWriter stream;
    private final ObjectMapper objectMapper;

    public PipesBufferedStreamMessageWriterChannel(OutputStream outputStream) {
        this.buffer = new ArrayList<>();
        this.stream = new BufferedWriter(new OutputStreamWriter(outputStream));
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void writeMessage(PipesMessage message) {
        buffer.add(message);
    }

    public void flush() throws IOException {
        try {
            // Iterate through buffered messages and write each one to the stream
            for (PipesMessage message : buffer) {
                String jsonMessage = objectMapper.writeValueAsString(message);
                stream.write(jsonMessage);
                stream.newLine();
            }
            buffer.clear();
        } finally {
            stream.flush();
        }
    }
}
