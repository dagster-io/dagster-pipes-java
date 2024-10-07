package pipes.utils;

import pipes.DagsterPipesException;

import java.util.Map;

public final class PipesUtils {

    private PipesUtils() {
    }

    public static <T> T assertEnvParamType (
        Map<String, ?> envParams,
        String key,
        Class<T> expectedType,
        Class<?> cls
    ) throws DagsterPipesException {
        Object value = envParams.get(key);

        if (!expectedType.isInstance(value)) {
            throw new DagsterPipesException (
                String.format(
                    "Invalid type for parameter %s passed from orchestration side to %s." +
                    "\nExpected %s, got %s.",
                    key,
                    cls.getSimpleName(),
                    expectedType.getSimpleName(),
                    value.getClass().getSimpleName()
                )
            );
        }

        return expectedType.cast(value);
    }
}
