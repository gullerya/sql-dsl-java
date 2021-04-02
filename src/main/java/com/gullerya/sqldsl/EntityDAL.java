package com.gullerya.sqldsl;

import java.lang.reflect.Modifier;
import java.util.Arrays;

import javax.persistence.Entity;
import javax.sql.DataSource;

import com.gullerya.sqldsl.api.statements.Delete;
import com.gullerya.sqldsl.api.statements.Insert;
import com.gullerya.sqldsl.api.statements.Select;
import com.gullerya.sqldsl.api.statements.Update;
import com.gullerya.sqldsl.impl.EntityDALImpl;

/**
 * API for Entities Data Access operations
 * <p>
 * - SQL statements (DELETE, INSERT, SELECT, UPDATE)
 */
public interface EntityDAL<T> extends Delete<T>, Insert<T>, Select<T>, Update<T> {

	/**
	 * Instantiate DAL for a specific entity with the given DataSource
	 *
	 * @param <T>        Entity class/type will define all the relevant setup via
	 *                   JPA annotations
	 * @param entityType Entity class (see ET above)
	 * @param dataSource DataSource parameter to access the DB
	 * @return DAL instance
	 */
	static <T> EntityDAL<T> of(Class<T> entityType, DataSource dataSource) {
		validateEntityType(entityType);
		validateDataSource(dataSource);

		try {
			return new EntityDALImpl<>(entityType, dataSource);
		} catch (Throwable t) {
			throw new IllegalStateException("failed to build entity metadata for " + entityType, t);
		}
	}

	private static void validateEntityType(Class<?> et) {
		if (et == null) {
			throw new IllegalArgumentException("entity type MUST NOT be NULL");
		}
		if (!Modifier.isPublic(et.getModifiers())) {
			throw new IllegalArgumentException("entity MUST be a public class, " + et + " isn't");
		}
		if (et.getAnnotation(Entity.class) == null) {
			throw new IllegalArgumentException("entity MUST be a annotated with '" + Entity.class + "', " + et + " isn't");
		}
		if (Arrays.stream(et.getDeclaredConstructors()).noneMatch(c -> c.getParameterCount() == 0 && Modifier.isPublic(c.getModifiers()))) {
			throw new IllegalArgumentException("entity MUST have a public parameter-less constructor, " + et + " doesn't");
		}
	}

	private static void validateDataSource(DataSource ds) {
		if (ds == null) {
			throw new IllegalArgumentException("data source MUST NOT be NULL");
		}
	}
}
