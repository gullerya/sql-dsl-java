package com.gullerya.sqldsl.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import javax.persistence.AttributeConverter;
import javax.persistence.Column;
import javax.persistence.Convert;

class EntityFieldMetadata {
	final Field field;
	final Column column;
	final AttributeConverter<Object, Object> converter;

	EntityFieldMetadata(Field field, Column column) throws ReflectiveOperationException {
		if (!Modifier.isPublic(field.getModifiers())) {
			field.setAccessible(true);
		}
		this.field = field;
		this.column = column;
		this.converter = obtainConverter(field);
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
