package pipes.utils;

import pipes.constants.Method;
import pipes.DagsterPipesException;
import pipes.PipesConstants;
import pipes.writers.PipesMessage;

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

    public static PipesMessage makeMessage(Method method, Map<String, Object> params) {
        return new PipesMessage(PipesConstants.PIPES_PROTOCOL_VERSION.name, method.toValue(), params);
    }
}
