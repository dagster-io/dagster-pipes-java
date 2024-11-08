package types;

import com.fasterxml.jackson.annotation.*;

public class PipesException {
    private PipesExceptionCause cause;
    private PipesExceptionContext context;
    private String message;
    private String name;
    private String[] stack;

    /**
     * exception that explicitly led to this exception
     */
    @JsonProperty("cause")
    public PipesExceptionCause getCause() { return cause; }
    @JsonProperty("cause")
    public void setCause(PipesExceptionCause value) { this.cause = value; }

    /**
     * exception that being handled when this exception was raised
     */
    @JsonProperty("context")
    public PipesExceptionContext getContext() { return context; }
    @JsonProperty("context")
    public void setContext(PipesExceptionContext value) { this.context = value; }

    @JsonProperty("message")
    public String getMessage() { return message; }
    @JsonProperty("message")
    public void setMessage(String value) { this.message = value; }

    /**
     * class name of Exception object
     */
    @JsonProperty("name")
    public String getName() { return name; }
    @JsonProperty("name")
    public void setName(String value) { this.name = value; }

    @JsonProperty("stack")
    public String[] getStack() { return stack; }
    @JsonProperty("stack")
    public void setStack(String[] value) { this.stack = value; }
}
