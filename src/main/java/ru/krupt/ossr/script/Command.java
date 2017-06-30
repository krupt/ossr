package ru.krupt.ossr.script;

import lombok.Value;

@Value
public class Command {

    private CommandType type;

    private CommandSubType subType;

    private ObjectType objectType;

    private String text;

    @Override
    public String toString() {
        return text;
    }
}
