package com.gullerya.typedsql.entities.test;

import com.gullerya.typedsql.configuration.DataSourceProvider;
import com.gullerya.typedsql.entities.EntitiesService;
import com.gullerya.typedsql.entities.Entity;
import com.gullerya.typedsql.entities.EntityField;
import org.junit.Test;

import javax.sql.DataSource;

public class ServiceTest {
	private final DataSource ds = DataSourceProvider.getInstance().getDataSourceDetails().getDataSource();

	@Test(expected = IllegalArgumentException.class)
	public void negA() {
		EntitiesService.of(null, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negB() {
		EntitiesService.of(Object.class, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negC() {
		EntitiesService.of(Object.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negD() {
		EntitiesService.of(EntityInvalidNotPublic.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negE() {
		EntitiesService.of(EntityInvalidNoCtorA.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negF() {
		EntitiesService.of(EntityInvalidNoCtorB.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negG() {
		EntitiesService.of(EntityInvalidNoFields.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negH() {
		EntitiesService.of(EntityInvalidFieldPrimitive.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negI() {
		EntitiesService.of(EntityInvalidFieldNotPublic.class, ds);
	}

	/**
	 * entities to test with
	 */
	@Entity(value = "table", schema = "schema")
	static class EntityInvalidNotPublic {
	}

	@Entity(value = "table", schema = "schema")
	public static class EntityInvalidNoCtorA {
		protected EntityInvalidNoCtorA() {
		}
	}

	@Entity(value = "table", schema = "schema")
	public static class EntityInvalidNoCtorB {
		public EntityInvalidNoCtorB(String some) {
		}
	}

	@Entity(value = "table")
	public static class EntityInvalidNoFields {
	}

	@Entity(value = "table", schema = "schema")
	public static class EntityInvalidFieldPrimitive {
		@EntityField("prim")
		public long l;
	}

	@Entity(value = "table")
	public static class EntityInvalidFieldNotPublic {
		@EntityField("pp")
		Long l;
	}
}
