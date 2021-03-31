// package com.gullerya.sqldsl.entities.test;

// import org.junit.Test;

// import javax.persistence.Entity;
// import javax.persistence.Table;
// import javax.sql.DataSource;

// import com.gullerya.sql._configuration.DataSourceProvider;
// import com.gullerya.sql.entities.EntityField;
// import com.gullerya.sql.entities.EntityService;

// import java.util.ArrayList;
// import java.util.Collection;

// public class InsertTest {
// 	private final DataSource ds = DataSourceProvider.getInstance().getDataSourceDetails().getDataSource();

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negA() {
// 		InsertTestEntity e = null;
// 		EntityDAL.of(InsertTestEntity.class, ds).insert(e);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negB() {
// 		Collection<InsertTestEntity> ec = null;
// 		EntityDAL.of(InsertTestEntity.class, ds).insert(ec);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negC() {
// 		Collection<InsertTestEntity> ec = new ArrayList<>();
// 		EntityDAL.of(InsertTestEntity.class, ds).insert(ec);
// 	}

// 	/**
// 	 * entities to test with
// 	 */
// 	@Entity
// 	@Table(name = "table", schema = "schema")
// 	public static final class InsertTestEntity {
// 		@EntityField("id")
// 		public Long id;
// 	}
// }
