package com.gullerya.sqldsl.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
			result = converter.convertToEntityAttribute(result);
		} else {
			if (fieldType.isArray()) {
				result = rs.getBytes(columnName);
			} else if (InputStream.class.isAssignableFrom(fieldType)) {
				try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
					rs.getBinaryStream(columnName).transferTo(baos);
					result = new ByteArrayInputStream(baos.toByteArray());
				} catch (IOException ioe) {
					throw new IllegalStateException("failed to process data stream for '" + columnName + "'");
				}
			} else if (fieldType == char.class || fieldType == Character.class) {
				result = rs.getString(columnName).charAt(0);
			} else if (fieldType == String.class) {
				result = rs.getString(columnName);
			} else if (fieldType == boolean.class || fieldType == Boolean.class) {
				result = rs.getBoolean(columnName);
			} else if (fieldType == byte.class || fieldType == Byte.class) {
				result = rs.getByte(columnName);
			} else if (fieldType == short.class || fieldType == Short.class) {
				result = rs.getShort(columnName);
			} else if (fieldType == int.class || fieldType == Integer.class) {
				result = rs.getInt(columnName);
			} else if (fieldType == long.class || fieldType == Long.class) {
				result = rs.getLong(columnName);
			} else if (fieldType == float.class || fieldType == Float.class) {
				result = rs.getFloat(columnName);
			} else if (fieldType == double.class || fieldType == Double.class) {
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

//	private Object getColumnValueByType(ResultSet rs, String columnName) throws SQLException {
//		Object result;
//		int columnIndex = rs.findColumn(columnName);
//		int columnType = rs.getMetaData().getColumnType(columnIndex);
//		switch (columnType) {
//			case Types.ARRAY:
//				result = rs.getArray(columnIndex);
//				break;
//			case Types.BIGINT:
//				result = rs.getLong(columnIndex);
//				break;
//			case Types.BINARY:
//				result = rs.getBinaryStream(columnIndex);
//				break;
//			case Types.BIT:
//				throw new IllegalStateException("unsupported type 'BIT'");
//			case Types.BLOB:
//				result = rs.getBlob(columnIndex);
//				break;
//			case Types.BOOLEAN:
//				result = rs.getBoolean(columnIndex);
//				break;
//			case Types.CHAR:
//				result = rs.getString(columnIndex);
//				break;
//			case Types.CLOB:
//				result = rs.getClob(columnIndex);
//				break;
//			case Types.DATE:
//				result = rs.getDate(columnIndex);
//				break;
//			case Types.DECIMAL:
//				result = rs.getBigDecimal(columnIndex);
//				break;
//			case Types.DOUBLE:
//				result = rs.getDouble(columnIndex);
//				break;
//			case Types.FLOAT:
//				result = rs.getFloat(columnIndex);
//				break;
//			case Types.INTEGER:
//				result = rs.getInt(columnIndex);
//				break;
//			case Types.LONGNVARCHAR:
//				result = rs.getNCharacterStream(columnIndex);
//				break;
//			case Types.LONGVARBINARY:
//				result = rs.getBinaryStream(columnIndex);
//				break;
//			case Types.LONGVARCHAR:
//				result = rs.getCharacterStream(columnIndex);
//				break;
//			case Types.NCHAR:
//				result = rs.getNString(columnIndex);
//				break;
//			case Types.NCLOB:
//				result = rs.getNClob(columnIndex);
//				break;
//			case Types.NVARCHAR:
//				result = rs.getNString(columnIndex);
//				break;
//			case Types.NUMERIC:
//				result = rs.getBigDecimal(columnIndex);
//				break;
//			case Types.VARBINARY:
//				result = rs.getBinaryStream(columnIndex);
//				break;
//			case Types.VARCHAR:
//				result = rs.getString(columnIndex);
//				break;
//			case Types.TINYINT:
//				result = rs.getByte(columnIndex);
//				break;
//			case Types.TIME:
//				result = rs.getTime(columnIndex);
//				break;
//			case Types.TIME_WITH_TIMEZONE:
//				result = rs.getTime(columnIndex);
//				break;
//			case Types.TIMESTAMP:
//				result = rs.getTimestamp(columnIndex);
//				break;
//			case Types.TIMESTAMP_WITH_TIMEZONE:
//				result = rs.getTimestamp(columnIndex);
//				break;
//			case Types.SMALLINT:
//				result = rs.getShort(columnIndex);
//				break;
//			case Types.SQLXML:
//				result = rs.getSQLXML(columnIndex);
//				break;
//			case Types.STRUCT:
//				throw new IllegalStateException("unsupported type 'STRUCT'");
//			case Types.REAL:
//				result = rs.getBigDecimal(columnIndex);
//				break;
//			case Types.REF:
//				throw new IllegalStateException("unsupported type 'REF'");
//			case Types.REF_CURSOR:
//				throw new IllegalStateException("unsupported type 'REF_CURSOR'");
//			case Types.ROWID:
//				result = rs.getRowId(columnIndex);
//				break;
//			default:
//				result = rs.getObject(columnIndex);
//		}
//		return result;
//	}
}
