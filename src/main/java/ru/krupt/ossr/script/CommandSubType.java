package ru.krupt.ossr.script;

public enum CommandSubType {

    INSERT,
    UPDATE,
    DELETE,

    CREATE,
    DROP,
    ALTER,
    COMMENT,
    GRANT,
    REVOKE,

    UNKNOWN;

    public static CommandSubType of(String command) {
        for (CommandSubType type : values()) {
            if (command.startsWith(type.name().toLowerCase())) {
                return type;
            }
        }
        return UNKNOWN;
    }
}
