package com.gullerya.sqldsl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import javax.sql.DataSource;

import com.gullerya.sqldsl.api.statements.Delete;
import com.gullerya.sqldsl.api.statements.Insert;
import com.gullerya.sqldsl.api.statements.Select;
import com.gullerya.sqldsl.api.statements.Update;
import com.gullerya.sqldsl.impl.EntityDALImpl;

/**
 * API for Entities Data Access operations
 * 
 * - SQL statements (DELETE, INSERT, SELECT, UPDATE)
 */
public interface EntityDAL<ET> extends Delete<ET>, Insert<ET>, Select<ET>, Update<ET> {

	/**
	 * Instantiate DAL for a specific entity with the given DataSource
	 * 
	 * @param <ET>       Entity class/type will define all the relevant setup via
	 *                   JPA annotations
	 * @param entityType Entity class (see ET above)
	 * @param dataSource DataSource parameter to access the DB
	 * @return DAL instance
	 */
	static <ET> EntityDAL<ET> of(Class<ET> entityType, DataSource dataSource) {
		if (entityType == null) {
			throw new IllegalArgumentException("entity type MUST NOT be NULL");
		}
		if (dataSource == null) {
			throw new IllegalArgumentException("data source MUST NOT be NULL");
		}
		if (!Modifier.isPublic(entityType.getModifiers())) {
			throw new IllegalArgumentException("entity MUST be a public class, " + entityType + " isn't");
		}
		boolean validCTor = false;
		for (Constructor<?> constructor : entityType.getDeclaredConstructors()) {
			if (constructor.getParameterCount() == 0 && Modifier.isPublic(constructor.getModifiers())) {
				validCTor = true;
				break;
			}
		}
		if (!validCTor) {
			throw new IllegalArgumentException(
					"entity MUST have a public parameter-less constructor, " + entityType + " doesn't");
		}

		try {
			return new EntityDALImpl<>(entityType, dataSource);
		} catch (Throwable t) {
			throw new IllegalStateException("failed to build entity metadata for " + entityType, t);
		}
	}
}
