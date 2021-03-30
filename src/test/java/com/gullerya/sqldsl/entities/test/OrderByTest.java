package com.gullerya.sqldsl.entities.test;

import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

import com.gullerya.sql._configuration.DataSourceProvider;
import com.gullerya.sql.entities.EntityField;
import com.gullerya.sql.entities.EntityService;
import com.gullerya.sql.entities.OrderBy;

public class OrderByTest {
	private final DataSource dataSource = DataSourceProvider.getInstance().getDataSourceDetails().getDataSource();

	@Test(expected = IllegalArgumentException.class)
	public void negativeA1() {
		OrderBy.asc(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeA2() {
		OrderBy.asc("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeA3() {
		OrderBy.desc(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeA4() {
		OrderBy.desc("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB1() {
		EntityService.of(TestEnt.class, dataSource).select("some")
				.orderBy(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB2() {
		EntityService.of(TestEnt.class, dataSource).select("some")
				.orderBy(OrderBy.asc("none"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB3() {
		EntityService.of(TestEnt.class, dataSource).select("some")
				.groupBy("some")
				.orderBy(OrderBy.asc("id"));
	}

	@Entity
	@Table(name = "some")
	public static class TestEnt {
		@EntityField("id")
		public String id;
		@EntityField("some")
		public String some;
	}
}
