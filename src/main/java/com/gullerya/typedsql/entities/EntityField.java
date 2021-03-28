package com.gullerya.typedsql.entities;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface EntityField {

	/**
	 * value bears the column name
	 */
	String value();

	/**
	 * readonly field will be removed from any UPDATE flow and effectively will stay as it was initially created
	 */
	boolean readonly() default false;

	/**
	 * non nullable field will raise an {@link IllegalArgumentException} in the following validations
	 * - in CREATE flow, if the field value is NULL
	 * - in UPDATE flow, if the field
	 */
	boolean nullable() default true;

	/**
	 * custom converter for a customized processing logic
	 * - this converter will transform DB types to/from custom application types
	 *
	 * @return converter implementation class
	 */
	Class<? extends TypeConverter> typeConverter() default DefaultTypeConverter.class;

	/**
	 * custom converter for a customized processing logic
	 * - this converter will work on JDBC objects, should be considered low level converter
	 *
	 * @return converter implementation class
	 */
	Class<? extends JdbcConverter> jdbcConverter() default JdbcConverter.class;


	interface TypeConverter<DB_TYPE, FIELD_TYPE> {
		DB_TYPE toDB(FIELD_TYPE input);

		FIELD_TYPE fromDB(DB_TYPE input);

		Class<DB_TYPE> getDbType();
	}

	final class DefaultTypeConverter implements TypeConverter<Object, Object> {
		static final TypeConverter<Object, Object> INSTANCE = new DefaultTypeConverter();

		@Override
		public Object toDB(Object input) {
			return input;
		}

		@Override
		public Object fromDB(Object input) {
			return input;
		}

		@Override
		public Class<Object> getDbType() {
			return Object.class;
		}
	}

	interface JdbcConverter<FIELD_TYPE> {
		void toDB(PreparedStatement preparedStatement, int index, FIELD_TYPE input) throws SQLException;

		FIELD_TYPE fromDB(ResultSet resultSet, int index) throws SQLException;
	}
}
