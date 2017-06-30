package ru.krupt.ossr.command;

import ru.krupt.ossr.script.CommandType;

public class DefaultSqlCommand implements CommandType {

    public char getTerminateChar() {
        return ';';
    }

    public boolean isTerminateCharEquals() {
        return false;
    }
}
