package pipes.writers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pipes.PipesMetadata;
import pipes.PipesTests;
import types.Method;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PipesFileMessageWriterChannelTest {

    @TempDir
    static Path tempDir;

    @Test
    void writeMessage() throws IOException {
        Map<String, ?> metadata = new PipesTests().buildTestMetadata();
        PipesMessage message = new PipesMessage(
            "0.1", Method.REPORT_ASSET_MATERIALIZATION.toValue(), metadata
        );
        Path filePath = tempDir.resolve("message.txt");
        PipesFileMessageWriterChannel fileWriter = new PipesFileMessageWriterChannel(
            filePath.toString()
        );
        fileWriter.writeMessage(message);
        String content = Files.readAllLines(filePath).get(0);
        assertEquals(
            content,
            "{\"__dagster_pipes_version\":\"0.1\"," +
            "\"method\":\"report_asset_materialization\"," +
            "\"params\":" +
                "{\"bool_true\":{\"value\":true,\"type\":\"bool\"}," +
                "\"float\":{\"value\":0.1,\"type\":\"float\"}," +
                "\"int\":{\"value\":1,\"type\":\"int\"}," +
                "\"url\":{\"value\":\"https://dagster.io\",\"type\":\"url\"}," +
                "\"path\":{\"value\":\"/dev/null\",\"type\":\"path\"}," +
                "\"null\":{\"value\":null,\"type\":\"null\"}," +
                "\"md\":{\"value\":\"**markdown**\",\"type\":\"md\"}," +
                "\"json\":{\"value\":{\"quux\":{\"a\":1,\"b\":2},\"corge\":null,\"qux\":[1,2,3],\"foo\":\"bar\",\"baz\":1},\"type\":\"json\"}," +
                "\"bool_false\":{\"value\":false,\"type\":\"bool\"}" +
                ",\"text\":{\"value\":\"hello\",\"type\":\"text\"}," +
                "\"asset\":{\"value\":[\"foo\",\"bar\"],\"type\":\"asset\"}," +
                "\"job\":{\"value\":\"my_other_job\",\"type\":\"job\"}," +
                "\"dagster_run\":{\"value\":\"db892d7f-0031-4747-973d-22e8b9095d9d\",\"type\":\"dagster_run\"}," +
                "\"notebook\":{\"value\":\"notebook.ipynb\",\"type\":\"notebook\"}}}"
        );
    }
}