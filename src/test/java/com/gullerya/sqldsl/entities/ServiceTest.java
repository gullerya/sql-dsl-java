// package com.gullerya.sqldsl.entities.test;

// import org.junit.Test;

// import javax.persistence.Entity;
// import javax.persistence.Table;
// import javax.sql.DataSource;

// import com.gullerya.sql._configuration.DataSourceProvider;
// import com.gullerya.sql.entities.EntityField;
// import com.gullerya.sql.entities.EntityService;

// public class ServiceTest {
// 	private final DataSource ds = DataSourceProvider.getInstance().getDataSourceDetails().getDataSource();

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negA() {
// 		EntityDAL.of(null, null);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negB() {
// 		EntityDAL.of(Object.class, null);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negC() {
// 		EntityDAL.of(Object.class, ds);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negD() {
// 		EntityDAL.of(EntityInvalidNotPublic.class, ds);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negE() {
// 		EntityDAL.of(EntityInvalidNoCtorA.class, ds);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negF() {
// 		EntityDAL.of(EntityInvalidNoCtorB.class, ds);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negG() {
// 		EntityDAL.of(EntityInvalidNoFields.class, ds);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negH() {
// 		EntityDAL.of(EntityInvalidFieldPrimitive.class, ds);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negI() {
// 		EntityDAL.of(EntityInvalidFieldNotPublic.class, ds);
// 	}

// 	/**
// 	 * entities to test with
// 	 */
// 	@Entity
// 	@Table(name = "table", schema = "schema")
// 	static class EntityInvalidNotPublic {
// 	}

// 	@Entity
// 	@Table(name = "table", schema = "schema")
// 	public static class EntityInvalidNoCtorA {
// 		protected EntityInvalidNoCtorA() {
// 		}
// 	}

// 	@Entity
// 	@Table(name = "table", schema = "schema")
// 	public static class EntityInvalidNoCtorB {
// 		public EntityInvalidNoCtorB(String some) {
// 		}
// 	}

// 	@Entity
// 	@Table(name = "table")
// 	public static class EntityInvalidNoFields {
// 	}

// 	@Entity
// 	@Table(name = "table", schema = "schema")
// 	public static class EntityInvalidFieldPrimitive {
// 		@EntityField("prim")
// 		public long l;
// 	}

// 	@Entity
// 	@Table(name = "table")
// 	public static class EntityInvalidFieldNotPublic {
// 		@EntityField("pp")
// 		Long l;
// 	}
// }
