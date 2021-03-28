package com.gullerya.typedsql.entities;

import javax.sql.DataSource;
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

public abstract class EntitiesService<T> implements Insert<T>, Select<T>, Update<T>, Delete<T> {
	private final ESConfig<T> config;

	private EntitiesService(Class<T> type, DataSource dataSource) throws ReflectiveOperationException {
		EntityMetadata<T> em = EntityMetadata.of(type);
		this.config = new ESConfig<>(dataSource, em);
	}

	public static <T> EntitiesService<T> of(Class<T> type, DataSource dataSource) {
		if (type == null) {
			throw new IllegalArgumentException("type MUST NOT be NULL");
		}
		if (dataSource == null) {
			throw new IllegalArgumentException("data source MUST NOT be NULL");
		}
		try {
			return new EntitiesServiceImpl<>(type, dataSource);
		} catch (ReflectiveOperationException roe) {
			throw new IllegalStateException("failed to build entity metadata for " + type);
		}
	}

	private static final class EntitiesServiceImpl<T> extends EntitiesService<T> {
		private EntitiesServiceImpl(Class<T> type, DataSource ds) throws ReflectiveOperationException {
			super(type, ds);
		}

		@Override
		public boolean insert(T entity, Literal... literals) {
			return new InsertImpl<>(super.config).insert(entity, literals);
		}

		@Override
		public int[] insert(Collection<T> entities, Literal... literals) {
			return new InsertImpl<>(super.config).insert(entities, literals);
		}

		@Override
		public SelectDownstream<T> select(String... fields) {
			return new SelectImpl<>(super.config).select(fields);
		}

		@Override
		public SelectDownstream<T> select(Set<String> fields) {
			return new SelectImpl<>(super.config).select(fields);
		}

		@Override
		public UpdateDownstream update(T entity, Literal... literals) {
			return new UpdateImpl<>(super.config).update(entity, literals);
		}

		@Override
		public int delete() {
			return new DeleteImpl<>(super.config).delete();
		}

		@Override
		public int delete(Where.WhereClause whereClause) {
			return new DeleteImpl<>(super.config).delete(whereClause);
		}
	}

	static final class EntityMetadata<T> {
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
			fqSchemaTableName = (!e.schema().isEmpty() ? ("\"" + (e.schema()) + "\".\"") : "\"") + e.value() + "\"";

			Map<String, FieldMetadata> tmpByFName = new HashMap<>();
			Map<String, FieldMetadata> tmpByColumn = new HashMap<>();
			for (Field f : type.getDeclaredFields()) {
				EntityField ef = f.getDeclaredAnnotation(EntityField.class);
				if (ef != null) {
					if (f.getType().isPrimitive()) {
						throw new IllegalArgumentException("entity field MAY NOT be of a primitive type, '" + f.getName() + "' of " + type + " is not");
					}
					if (!Modifier.isPublic(f.getModifiers())) {
						throw new IllegalArgumentException("entity field MUST be public, '" + f.getName() + "' of " + type + " is not");
					}
					FieldMetadata fm = new FieldMetadata(f, ef);
					tmpByFName.put(f.getName(), fm);
					tmpByColumn.put(ef.value(), fm);
				}
			}
			if (tmpByFName.isEmpty()) {
				throw new IllegalArgumentException("type " + type + " MUST have at least 1 fields annotated with " + EntityField.class);
			}
			byFName = tmpByFName.entrySet().stream()
					.sorted(Map.Entry.comparingByKey())
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
			byColumn = tmpByColumn.entrySet().stream()
					.sorted(Map.Entry.comparingByKey())
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
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
				throw new IllegalArgumentException("entity MUST have a public parameter-less constructor, " + type + " doesn't");
			}
		}
	}

	static final class FieldMetadata {
		final Field field;
		final EntityField fieldMetadata;
		final EntityField.TypeConverter<Object, Object> typeConverter;
		final EntityField.JdbcConverter<Object> jdbcConverter;

		@SuppressWarnings("unchecked")
		private FieldMetadata(Field field, EntityField fieldMetadata) throws ReflectiveOperationException {
			this.field = field;
			this.fieldMetadata = fieldMetadata;
			if (fieldMetadata.typeConverter() == EntityField.DefaultTypeConverter.class) {
				this.typeConverter = EntityField.DefaultTypeConverter.INSTANCE;
			} else {
				this.typeConverter = fieldMetadata.typeConverter().getDeclaredConstructor().newInstance();
			}
			if (fieldMetadata.jdbcConverter() != EntityField.JdbcConverter.class) {
				this.jdbcConverter = fieldMetadata.jdbcConverter().getDeclaredConstructor().newInstance();
			} else {
				this.jdbcConverter = null;
			}
		}
	}

	static final class ESConfig<T> {
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

	interface PreparedStatementAction<R> {
		R execute(PreparedStatement s) throws SQLException;
	}
}
