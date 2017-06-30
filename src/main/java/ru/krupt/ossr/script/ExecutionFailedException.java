package ru.krupt.ossr.script;

public class ExecutionFailedException extends RuntimeException {

    public ExecutionFailedException(String message) {
        super(message, null, false, false);
    }
}
