package ru.krupt.ossr.script;

import ru.krupt.ossr.util.StringUtils;

public enum ObjectType {

    TABLE,
    SEQUENCE,
    SYNONYM,
    INDEX,

    PACKAGE_BODY,
    PACKAGE,
    TYPE_BODY,
    TYPE,
    PROCEDURE,
    FUNCTION,
    TRIGGER,

    UNKNOWN;

    public static ObjectType of(String text) {
        text = StringUtils.toSingleSpaces(text.toLowerCase());
        for (ObjectType objectType : values()) {
            if (text.contains(" " + objectType.name().toLowerCase().replace('_', ' ') + " ")) {
                return objectType;
            }
        }
        return UNKNOWN;
    }
}
