package com.gullerya.typedsql.entities;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Entity {

	/**
	 * value bears the table name
	 */
	String value();

	/**
	 * schema name, if any
	 */
	String schema() default "";
}
