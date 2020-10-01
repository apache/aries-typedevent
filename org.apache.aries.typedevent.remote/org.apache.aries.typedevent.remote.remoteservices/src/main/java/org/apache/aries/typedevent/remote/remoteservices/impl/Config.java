package org.apache.aries.typedevent.remote.remoteservices.impl;

public @interface Config {

    public static enum Selector {
        ALL, WITH_FILTER, WITH_PROPERTY, CUSTOM;
    }
    
    public Selector listener_selection() default Selector.WITH_PROPERTY;
    
    public String listener_selection_custom_filter();
    
}
