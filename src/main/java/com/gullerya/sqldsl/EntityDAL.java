package com.gullerya.sqldsl;

import javax.persistence.AttributeConverter;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

import com.gullerya.sqldsl.api.clauses.Where.WhereClause;
import com.gullerya.sqldsl.api.statements.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Factory class and primaty SQL statement initiator:
 * 
 * - create instance dedicated to a specific entity with a specific DataSource
 * 
 * - initiate primary SQL statements (DELETE, INSERT, SELECT, UPDATE)
 */
public interface EntityDAL<ET> extends Delete<ET>, Insert<ET>, Select<ET>, Update<ET> {

	public static <ET> EntityDAL<ET> of(Class<ET> entityType, DataSource dataSource) {
		if (entityType == null) {
			throw new IllegalArgumentException("entity type MUST NOT be NULL");
		}
		if (dataSource == null) {
			throw new IllegalArgumentException("data source MUST NOT be NULL");
		}
		try {
			return new EntityDALImpl<>(entityType, dataSource);
		} catch (ReflectiveOperationException roe) {
			throw new IllegalStateException("failed to build entity metadata for " + entityType);
		}
	}

	// TODO: externalize?
	public static final class EntityDALImpl<ET> implements EntityDAL<ET> {
		private final ESConfig<ET> config;

		private EntityDALImpl(Class<ET> entityType, DataSource ds) throws ReflectiveOperationException {
			EntityMetadata<ET> em = EntityMetadata.of(entityType);
			this.config = new ESConfig<>(ds, em);
		}

		@Override
		public int delete() {
			// return new DeleteImpl<>(super.config).delete();
			return 0;
		}

		@Override
		public int delete(WhereClause whereClause) {
			// return new DeleteImpl<>(super.config).delete(whereClause);
			return 0;
		}

		@Override
		public boolean insert(ET entity, Literal... literals) {
			// return new InsertImpl<>(super.config).insert(entity, literals);
			return false;
		}

		@Override
		public int[] insert(Collection<ET> entities, Literal... literals) {
			// return new InsertImpl<>(super.config).insert(entities, literals);
			return null;
		}

		@Override
		public SelectDownstream<ET> select(String... fields) {
			// return new SelectImpl<>(super.config).select(fields);
			return null;
		}

		@Override
		public SelectDownstream<ET> select(Set<String> fields) {
			// return new SelectImpl<>(super.config).select(fields);
			return null;
		}

		@Override
		public UpdateDownstream update(ET entity, Literal... literals) {
			// return new UpdateImpl<>(super.config).update(entity, literals);
			return null;
		}
	}

	public static final class EntityMetadata<T> {
		private static final Map<Class<?>, EntityMetadata<?>> cache = new HashMap<>();
		private static final Object CREATE_LOCK = new Object();

		final Class<T> type;
		final String fqSchemaTableName;
		final Map<String, FieldMetadata> byFName;
		final Map<String, FieldMetadata> byColumn;

		@SuppressWarnings("unchecked")
		private static <T> EntityMetadata<T> of(Class<T> type) throws ReflectiveOperationException {
			EntityMetadata<?> result = cache.get(type);
			if (result == null) {
				synchronized (CREATE_LOCK) {
					result = cache.get(type);
					if (result == null) {
						result = new EntityMetadata<>(type);
						cache.put(type, result);
					}
				}
			}
			return (EntityMetadata<T>) result;
		}

		private EntityMetadata(Class<T> type) throws ReflectiveOperationException {
			validateEntityType(type);
			this.type = type;

			Entity e = type.getDeclaredAnnotation(Entity.class);
			if (e == null) {
				throw new IllegalArgumentException("type " + type + " MUST be annotated with " + Entity.class);
			}
			String tmpName = !e.name().isEmpty() ? e.name() : type.getSimpleName();
			Table t = type.getDeclaredAnnotation(Table.class);
			if (t != null) {
				tmpName = !t.name().isEmpty() ? t.name() : tmpName;
				fqSchemaTableName = "\"" + (!t.schema().isEmpty() ? (t.schema() + "\".\"") : "") + tmpName + "\"";
			} else {
				fqSchemaTableName = "\"" + tmpName + "\"";
			}

			Map<String, FieldMetadata> tmpByFName = new HashMap<>();
			Map<String, FieldMetadata> tmpByColumn = new HashMap<>();
			for (Field f : type.getDeclaredFields()) {
				Column ef = f.getDeclaredAnnotation(Column.class);
				if (ef != null) {
					// if (f.getType().isPrimitive()) {
					// throw new IllegalArgumentException("entity field MAY NOT be of a primitive
					// type, '"
					// + f.getName() + "' of " + type + " is not");
					// }
					FieldMetadata fm = new FieldMetadata(f, ef);
					tmpByFName.put(f.getName(), fm);
					tmpByColumn.put(ef.name(), fm);
				}
			}
			if (tmpByFName.isEmpty()) {
				throw new IllegalArgumentException(
						"type " + type + " MUST have at least 1 fields annotated with " + Column.class);
			}
			byFName = tmpByFName.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(
					Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
			byColumn = tmpByColumn.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(
					Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
		}

		private void validateEntityType(Class<T> type) {
			if (!Modifier.isPublic(type.getModifiers())) {
				throw new IllegalArgumentException("entity MUST be a public class, " + type + " isn't");
			}

			boolean validCTor = false;
			for (Constructor<?> constructor : type.getDeclaredConstructors()) {
				if (constructor.getParameterCount() == 0 && Modifier.isPublic(constructor.getModifiers())) {
					validCTor = true;
					break;
				}
			}
			if (!validCTor) {
				throw new IllegalArgumentException(
						"entity MUST have a public parameter-less constructor, " + type + " doesn't");
			}
		}
	}

	public static final class FieldMetadata {
		final Field field;
		final Column column;
		final AttributeConverter<?, ?> converter;

		private FieldMetadata(Field field, Column column) throws ReflectiveOperationException {
			this.field = field;
			this.column = column;
			this.converter = obtainConverter(field);
		}

		@SuppressWarnings("unchecked")
		private AttributeConverter<?, ?> obtainConverter(Field field) throws ReflectiveOperationException {
			AttributeConverter<?, ?> result = DefaultConverter.INSTANCE;
			Convert convert = field.getAnnotation(Convert.class);
			if (convert != null && !convert.disableConversion()) {
				Class<?> converterClass = convert.converter();
				if (converterClass == null) {
					throw new IllegalStateException("enabled converter MUST specify non-NULL converter");
				}
				if (!AttributeConverter.class.isAssignableFrom(converterClass)) {
					throw new IllegalStateException("converter MUST be a subclass of AttributeConverter");
				}
				result = ((Class<AttributeConverter<?, ?>>) converterClass).getDeclaredConstructor().newInstance();
			}
			return result;
		}
	}

	public static final class ESConfig<T> {
		private final DataSource ds;
		final EntityMetadata<T> em;

		private ESConfig(DataSource ds, EntityMetadata<T> em) {
			this.ds = ds;
			this.em = em;
		}

		<R> R preparedStatementAndDo(String sql, PreparedStatementAction<R> psAction) {
			try (Connection c = ds.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
				return psAction.execute(s);
			} catch (SQLException sqle) {
				throw new IllegalStateException(sqle);
			}
		}
	}

	public static final class DefaultConverter implements AttributeConverter<Object, Object> {
		private static final AttributeConverter<Object, Object> INSTANCE = new DefaultConverter();

		@Override
		public Object convertToDatabaseColumn(Object attribute) {
			return attribute;
		}

		@Override
		public Object convertToEntityAttribute(Object dbData) {
			return dbData;
		}
	}

	interface PreparedStatementAction<R> {
		R execute(PreparedStatement s) throws SQLException;
	}
}
