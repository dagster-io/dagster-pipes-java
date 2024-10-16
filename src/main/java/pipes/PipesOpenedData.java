package pipes;

import java.util.HashMap;
import java.util.Map;

public class PipesOpenedData {

    private final Map<String, Object> extras;

    public PipesOpenedData(Map<String, Object> extras) {
        this.extras = extras != null ? extras : new HashMap<>();
    }

    public Map<String, Object> getExtras() {
        return extras;
    }

}
