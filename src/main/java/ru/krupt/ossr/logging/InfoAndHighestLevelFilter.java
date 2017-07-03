package ru.krupt.ossr.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.filter.AbstractMatcherFilter;
import ch.qos.logback.core.spi.FilterReply;

public class InfoAndHighestLevelFilter extends AbstractMatcherFilter {

    public FilterReply decide(Object event) {
        final LoggingEvent loggingEvent = (LoggingEvent) event;
        final Level level = loggingEvent.getLevel();
        if (level == Level.TRACE || level == Level.DEBUG) {
            return FilterReply.DENY;
        }
        return FilterReply.ACCEPT;
    }
}
