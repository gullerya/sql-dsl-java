package com.gullerya.typedsql.entities.test;

import com.gullerya.typedsql.configuration.DataSourceProvider;
import com.gullerya.typedsql.entities.EntityService;
import com.gullerya.typedsql.entities.EntityField;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

public class ServiceTest {
	private final DataSource ds = DataSourceProvider.getInstance().getDataSourceDetails().getDataSource();

	@Test(expected = IllegalArgumentException.class)
	public void negA() {
		EntityService.of(null, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negB() {
		EntityService.of(Object.class, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negC() {
		EntityService.of(Object.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negD() {
		EntityService.of(EntityInvalidNotPublic.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negE() {
		EntityService.of(EntityInvalidNoCtorA.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negF() {
		EntityService.of(EntityInvalidNoCtorB.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negG() {
		EntityService.of(EntityInvalidNoFields.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negH() {
		EntityService.of(EntityInvalidFieldPrimitive.class, ds);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negI() {
		EntityService.of(EntityInvalidFieldNotPublic.class, ds);
	}

	/**
	 * entities to test with
	 */
	@Entity
	@Table(name = "table", schema = "schema")
	static class EntityInvalidNotPublic {
	}

	@Entity
	@Table(name = "table", schema = "schema")
	public static class EntityInvalidNoCtorA {
		protected EntityInvalidNoCtorA() {
		}
	}

	@Entity
	@Table(name = "table", schema = "schema")
	public static class EntityInvalidNoCtorB {
		public EntityInvalidNoCtorB(String some) {
		}
	}

	@Entity
	@Table(name = "table")
	public static class EntityInvalidNoFields {
	}

	@Entity
	@Table(name = "table", schema = "schema")
	public static class EntityInvalidFieldPrimitive {
		@EntityField("prim")
		public long l;
	}

	@Entity
	@Table(name = "table")
	public static class EntityInvalidFieldNotPublic {
		@EntityField("pp")
		Long l;
	}
}
