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

		Class<?> targetType = fieldType;
		if (converter != null) {
			targetType = obtainConverterTargetType(converter);
		}

		result = extractColumnValueByTargetType(rs, columnName, targetType);
		if (converter != null) {
			result = converter.convertToEntityAttribute(result);
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

	private static String obtainColumnName(Field field, Column column) {
		String result = column.name();
		if (result.isEmpty()) {
			result = field.getName();
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private static AttributeConverter<Object, Object> obtainConverter(Field f) throws ReflectiveOperationException {
		AttributeConverter<Object, Object> result = null;
		Convert convert = f.getAnnotation(Convert.class);
		if (convert != null && !convert.disableConversion()) {
			Class<AttributeConverter<Object, Object>> converterClass = convert.converter();
			if (!AttributeConverter.class.isAssignableFrom(converterClass)) {
				throw new IllegalStateException("converter MUST be a subclass of AttributeConverter");
			}
			result = converterClass.getDeclaredConstructor().newInstance();
		}
		return result;
	}

	private static Class<?> obtainConverterTargetType(AttributeConverter<?, ?> c) {
		Class<?> result = null;
		Optional<ParameterizedType> pt = Arrays.stream(c.getClass().getGenericInterfaces())
				.filter(i -> i instanceof ParameterizedType)
				.map(i -> (ParameterizedType) i)
				.filter(i -> i.getRawType().getTypeName().startsWith(AttributeConverter.class.getTypeName()))
				.findFirst();
		if (pt.isPresent()) {
			result = (Class<?>) pt.get().getActualTypeArguments()[1];
		}
		return result;
	}

	private static Object extractColumnValueByTargetType(ResultSet rs, String cn, Class<?> tt) throws SQLException {
		Object result;
		if (tt.isArray()) {
			result = rs.getBytes(cn);
		} else if (InputStream.class.isAssignableFrom(tt)) {
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
				rs.getBinaryStream(cn).transferTo(baos);
				result = new ByteArrayInputStream(baos.toByteArray());
			} catch (IOException ioe) {
				throw new IllegalStateException("failed to process data stream for '" + cn + "'");
			}
		} else if (tt == char.class || tt == Character.class) {
			result = rs.getString(cn).charAt(0);
		} else if (tt == boolean.class || tt == Boolean.class) {
			result = rs.getBoolean(cn);
		} else if (tt == byte.class || tt == Byte.class) {
			result = rs.getByte(cn);
		} else if (tt == short.class || tt == Short.class) {
			result = rs.getShort(cn);
		} else if (tt == int.class || tt == Integer.class) {
			result = rs.getInt(cn);
		} else if (tt == long.class || tt == Long.class) {
			result = rs.getLong(cn);
		} else if (tt == float.class || tt == Float.class) {
			result = rs.getFloat(cn);
		} else if (tt == double.class || tt == Double.class) {
			result = rs.getDouble(cn);
		} else {
			result = rs.getObject(cn, tt);
		}
		return result;
	}

//	private Object getColumnValueByDBType(ResultSet rs, String columnName) throws SQLException {
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
