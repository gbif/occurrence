package org.gbif.occurrence.ws.app;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface PropertiesKey {

  String value() default "";

}
