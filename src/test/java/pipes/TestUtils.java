package pipes;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestUtils {

    public static Set<Map.Entry<String, String>> sanitizeMapEntries(Map<String, String> map) {
        return map.entrySet().stream()
            .map(entry -> new AbstractMap.SimpleEntry<>(
                    entry.getKey(),
                    removeTrailingQuotes(entry.getValue()))
            )
            .collect(Collectors.toSet());
    }

    public static String removeTrailingQuotes(String value) {
        if (value == null) {
            return null;
        }
        return value.replaceAll("^\"|\"$", "");
    }
}
