package com.gullerya.sqldsl.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import javax.persistence.AttributeConverter;
import javax.persistence.Column;
import javax.persistence.Convert;

class EntityFieldMetadata {
	final Class<?> fieldType;
	private final Field field;

	final String columnName;
	final Column column;

	private final AttributeConverter<Object, Object> converter;

	EntityFieldMetadata(Field field, Column column) throws ReflectiveOperationException {
		if (!Modifier.isPublic(field.getModifiers())) {
			field.setAccessible(true);
		}
		this.fieldType = field.getType();

		this.columnName = obtainColumnName(field, column);
		this.column = column;

		this.field = field;
		this.converter = obtainConverter(field);
	}

	Object getFieldValue(Object entity) {
		try {
			return this.field.get(entity);
		} catch (ReflectiveOperationException roe) {
			throw new IllegalStateException("failed to get field value from entity", roe);
		}
	}

	void setFieldValue(Object entity, Object value) {
		try {
			this.field.set(entity, value);
		} catch (ReflectiveOperationException roe) {
			throw new IllegalStateException("failed to set field value on entity", roe);
		}
	}

	private String obtainColumnName(Field field, Column column) {
		String result = column.name();
		if (result.isEmpty()) {
			result = field.getName();
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private AttributeConverter<Object, Object> obtainConverter(Field field) throws ReflectiveOperationException {
		AttributeConverter<Object, Object> result = null;
		Convert convert = field.getAnnotation(Convert.class);
		if (convert != null && !convert.disableConversion()) {
			Class<AttributeConverter<Object, Object>> converterClass = convert.converter();
			if (!AttributeConverter.class.isAssignableFrom(converterClass)) {
				throw new IllegalStateException("converter MUST be a subclass of AttributeConverter");
			}
			result = converterClass.getDeclaredConstructor().newInstance();
		}
		return result;
	}
}
