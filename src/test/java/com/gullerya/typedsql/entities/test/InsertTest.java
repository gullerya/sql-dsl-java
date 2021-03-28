package com.gullerya.typedsql.entities.test;

import com.gullerya.typedsql.configuration.DataSourceProvider;
import com.gullerya.typedsql.entities.EntityService;
import com.gullerya.typedsql.entities.EntityField;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collection;

public class InsertTest {
	private final DataSource ds = DataSourceProvider.getInstance().getDataSourceDetails().getDataSource();

	@Test(expected = IllegalArgumentException.class)
	public void negA() {
		InsertTestEntity e = null;
		EntityService.of(InsertTestEntity.class, ds).insert(e);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negB() {
		Collection<InsertTestEntity> ec = null;
		EntityService.of(InsertTestEntity.class, ds).insert(ec);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negC() {
		Collection<InsertTestEntity> ec = new ArrayList<>();
		EntityService.of(InsertTestEntity.class, ds).insert(ec);
	}

	/**
	 * entities to test with
	 */
	@Entity
	@Table(name = "table", schema = "schema")
	public static final class InsertTestEntity {
		@EntityField("id")
		public Long id;
	}
}
