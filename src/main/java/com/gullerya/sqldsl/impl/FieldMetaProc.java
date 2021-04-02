package com.gullerya.sqldsl.impl;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;

import javax.persistence.AttributeConverter;
import javax.persistence.Column;
import javax.persistence.Convert;

class FieldMetaProc {
	private final Class<?> fieldType;
	private final Field field;

	final String columnName;
	final Column column;

	private final AttributeConverter<Object, Object> converter;

	FieldMetaProc(Field field, Column column) throws ReflectiveOperationException {
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

	Object getColumnValue(ResultSet rs, String columnName) throws SQLException {
		Object result;
		if (converter != null) {
			Optional<ParameterizedType> pt = Arrays.stream(converter.getClass().getGenericInterfaces())
					.filter(i -> i instanceof ParameterizedType)
					.map(i -> (ParameterizedType) i)
					.filter(i -> i.getRawType().getTypeName().startsWith(AttributeConverter.class.getTypeName()))
					.findFirst();
			if (pt.isPresent()) {
				result = converter.convertToEntityAttribute(rs.getObject(columnName, (Class<?>) pt.get().getActualTypeArguments()[0]));
			} else {
				result = converter.convertToEntityAttribute(rs.getObject(columnName));
			}
		} else {
			if (fieldType.isArray()) {
				result = rs.getBytes(columnName);
			} else if (InputStream.class.isAssignableFrom(fieldType)) {
				result = rs.getBinaryStream(columnName);
			} else if (boolean.class.isAssignableFrom(fieldType)) {
				result = rs.getBoolean(columnName);
			} else if (byte.class.isAssignableFrom(fieldType)) {
				result = rs.getByte(columnName);
			} else if (short.class.isAssignableFrom(fieldType)) {
				result = rs.getShort(columnName);
			} else if (int.class.isAssignableFrom(fieldType)) {
				result = rs.getInt(columnName);
			} else if (long.class.isAssignableFrom(fieldType)) {
				result = rs.getLong(columnName);
			} else if (float.class.isAssignableFrom(fieldType)) {
				result = rs.getFloat(columnName);
			} else if (double.class.isAssignableFrom(fieldType)) {
				result = rs.getDouble(columnName);
			} else {
				result = rs.getObject(columnName, fieldType);
			}
		}
		return result;
	}

	void setColumnValue(PreparedStatement ps, int index, Object value) throws SQLException {
		if (converter != null) {
			ps.setObject(index, converter.convertToDatabaseColumn(value));
		} else {
			if (value instanceof InputStream) {
				ps.setBinaryStream(index, (InputStream) value);
			} else if (value instanceof byte[]) {
				ps.setBytes(index, (byte[]) value);
			} else {
				ps.setObject(index, value);
			}
		}
	}

	Object translateFieldToColumn(Object field) {
		return converter != null ? converter.convertToDatabaseColumn(field) : field;
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
