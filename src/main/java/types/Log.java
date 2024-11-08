package types;

import java.io.IOException;
import com.fasterxml.jackson.annotation.*;

public enum Log {
    CRITICAL, DEBUG, ERROR, EXCEPTION, INFO, WARNING;

    @JsonValue
    public String toValue() {
        switch (this) {
            case CRITICAL: return "CRITICAL";
            case DEBUG: return "DEBUG";
            case ERROR: return "ERROR";
            case EXCEPTION: return "EXCEPTION";
            case INFO: return "INFO";
            case WARNING: return "WARNING";
        }
        return null;
    }

    @JsonCreator
    public static Log forValue(String value) throws IOException {
        if (value.equals("CRITICAL")) return CRITICAL;
        if (value.equals("DEBUG")) return DEBUG;
        if (value.equals("ERROR")) return ERROR;
        if (value.equals("EXCEPTION")) return EXCEPTION;
        if (value.equals("INFO")) return INFO;
        if (value.equals("WARNING")) return WARNING;
        throw new IOException("Cannot deserialize Log");
    }
}
