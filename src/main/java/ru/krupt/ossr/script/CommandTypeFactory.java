package ru.krupt.ossr.script;

class CommandTypeFactory {

    static CommandType getCommand(String text) {
        if (text.startsWith("declare") || text.startsWith("begin")) {
            return CommandTypes.ANONYMOUS_PLSQL_BLOCK;
        } else if (matchesPlSqlObject(text)) {
            return CommandTypes.PLSQL_OBJECT;
        }
        return CommandTypes.DEFAULT;
    }

    private static boolean matchesPlSqlObject(String source) {
        if (source.startsWith("create ")) {
            if (source.contains(" type ") || source.contains(" package ") || source.contains(" function ")
                    || source.contains(" procedure ") || source.contains(" trigger ") || source.contains(" java ")) {
                return true;
            }
        }
        return false;
    }
}
