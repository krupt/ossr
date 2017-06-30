package ru.krupt.ossr.command;

import ru.krupt.ossr.script.CommandType;

public class AnonymousPlSqlBlock implements CommandType {

    public char getTerminateChar() {
        return '/';
    }

    public boolean isTerminateCharEquals() {
        return true;
    }
}
