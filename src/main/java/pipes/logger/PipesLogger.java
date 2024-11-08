package pipes.logger;

import pipes.utils.PipesUtils;
import types.Method;
import types.PipesLog;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class PipesLogger {

    private final Logger logger;

    public PipesLogger(Logger logger) {
        this.logger = logger;
    }

    public void debug(String message) {
        log(new PipesLogLevel(PipesLog.INFO.toValue(), 500), message);
    }

    public void info(String message) {
        log(new PipesLogLevel(PipesLog.INFO.toValue(), 800), message);
    }

    public void warning(String message) {
        log(new PipesLogLevel(PipesLog.INFO.toValue(), 900), message);
    }

    public void exception(String message) {
        log(new PipesLogLevel(PipesLog.INFO.toValue(), 950), message);
    }

    public void error(String message) {
        log(new PipesLogLevel(PipesLog.INFO.toValue(), 1000), message);
    }

    public void critical(String message) {
        log(new PipesLogLevel(PipesLog.INFO.toValue(), 1100), message);
    }

    private void log(PipesLogLevel level, String message) {
        Map<String, String> logRecordParams = new HashMap<>();
        logRecordParams.put("message", message);
        logRecordParams.put("level", level.getName());
        PipesUtils.makeMessage(Method.LOG, logRecordParams);
        this.logger.log(new LogRecord(level, message));
    }
}
