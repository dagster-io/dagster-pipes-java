package pipes.constants;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.IOException;

/**
 * Event type
 */
public enum Method {
    CLOSED, LOG, OPENED, REPORT_ASSET_CHECK, REPORT_ASSET_MATERIALIZATION, REPORT_CUSTOM_MESSAGE;

    @JsonValue
    public String toValue() {
        return switch (this) {
            case CLOSED -> "closed";
            case LOG -> "log";
            case OPENED -> "opened";
            case REPORT_ASSET_CHECK -> "report_asset_check";
            case REPORT_ASSET_MATERIALIZATION -> "report_asset_materialization";
            case REPORT_CUSTOM_MESSAGE -> "report_custom_message";
        };
    }

    @JsonCreator
    public static Method forValue(String value) throws IOException {
        return switch (value) {
            case "closed" -> CLOSED;
            case "log" -> LOG;
            case "opened" -> OPENED;
            case "report_asset_check" -> REPORT_ASSET_CHECK;
            case "report_asset_materialization" -> REPORT_ASSET_MATERIALIZATION;
            case "report_custom_message" -> REPORT_CUSTOM_MESSAGE;
            default -> throw new IOException("Cannot deserialize Method");
        };
    }
}
