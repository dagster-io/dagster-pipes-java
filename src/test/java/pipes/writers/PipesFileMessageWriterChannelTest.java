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
            "{\"__dagster_pipes_version\":\"0.1\",\"method\":\"report_asset_materialization\",\"params\":{\"bool_true\":{\"raw_value\":true,\"type\":\"bool\"},\"float\":{\"raw_value\":0.1,\"type\":\"float\"},\"int\":{\"raw_value\":1,\"type\":\"int\"},\"url\":{\"raw_value\":\"https://dagster.io\",\"type\":\"url\"},\"path\":{\"raw_value\":\"/dev/null\",\"type\":\"path\"},\"null\":{\"raw_value\":null,\"type\":\"null\"},\"md\":{\"raw_value\":\"**markdown**\",\"type\":\"md\"},\"json\":{\"raw_value\":{\"quux\":{\"a\":1,\"b\":2},\"corge\":null,\"qux\":[1,2,3],\"foo\":\"bar\",\"baz\":1},\"type\":\"json\"},\"bool_false\":{\"raw_value\":false,\"type\":\"bool\"},\"text\":{\"raw_value\":\"hello\",\"type\":\"text\"},\"asset\":{\"raw_value\":[\"foo\",\"bar\"],\"type\":\"asset\"},\"job\":{\"raw_value\":\"my_other_job\",\"type\":\"job\"},\"dagster_run\":{\"raw_value\":\"db892d7f-0031-4747-973d-22e8b9095d9d\",\"type\":\"dagster_run\"},\"notebook\":{\"raw_value\":\"notebook.ipynb\",\"type\":\"notebook\"}}}"
        );
    }
}