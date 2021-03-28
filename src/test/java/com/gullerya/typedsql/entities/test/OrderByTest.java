package com.gullerya.typedsql.entities.test;

import com.gullerya.typedsql.configuration.DataSourceProvider;
import com.gullerya.typedsql.entities.EntitiesService;
import com.gullerya.typedsql.entities.EntityField;
import com.gullerya.typedsql.entities.OrderBy;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

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
		EntitiesService.of(TestEnt.class, dataSource).select("some")
				.orderBy(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB2() {
		EntitiesService.of(TestEnt.class, dataSource).select("some")
				.orderBy(OrderBy.asc("none"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB3() {
		EntitiesService.of(TestEnt.class, dataSource).select("some")
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
