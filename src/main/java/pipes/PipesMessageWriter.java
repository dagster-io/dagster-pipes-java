package pipes;

import java.util.Map;

public abstract class PipesMessageWriter<T extends PipesMessageWriterChannel> {

    public abstract T open(Map<String, Object> params) throws DagsterPipesException;

    public final PipesOpenedData getOpenedPayload() {
        return new PipesOpenedData(getOpenedExtras().getExtras());
    }

    public PipesExtras getOpenedExtras() {
        return new PipesExtras();
    }
}
