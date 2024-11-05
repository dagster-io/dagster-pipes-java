package pipes;

import types.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PipesMetadata {

    private Object value;
    private Type type;

    public static final List<Class<?>> ALLOWED_VALUE_TYPES = Arrays.asList(
        Long.class, Double.class, Map.class,
        String[].class, Boolean.class, String.class
    );

    public PipesMetadata(Object value, Type type) {
        if (value != null && ALLOWED_VALUE_TYPES.stream().noneMatch(vt -> vt.isInstance(value))) {
            throw new IllegalArgumentException(String.format(
                "Wrong metadata value type: %s", value.getClass().getTypeName()
            ));
        }
        if (value instanceof Map && ((Map<?, ?>) value).keySet().stream().anyMatch(k -> !(k instanceof String))) {
            throw new IllegalArgumentException(String.format(
                "Wrong metadata value map type. Only String keys allowed: %s", value.getClass().getTypeName()
            ));
        }
        this.value = value;
        this.type = type;
    }
}
