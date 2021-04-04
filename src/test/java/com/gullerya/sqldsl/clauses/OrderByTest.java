package com.gullerya.sqldsl.clauses;

import com.gullerya.sqldsl.DBUtils;
import com.gullerya.sqldsl.EntityDAL;
import com.gullerya.sqldsl.api.clauses.OrderBy;
import org.junit.jupiter.api.Test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class OrderByTest {
	private final DataSource dataSource = DBUtils.getDataSource();

	@Test
	public void negativeA1() {
		assertThrows(
				IllegalArgumentException.class,
				() -> OrderBy.asc(null)
		);
	}

	@Test
	public void negativeA2() {
		assertThrows(
				IllegalArgumentException.class,
				() -> OrderBy.asc("")
		);
	}

	@Test
	public void negativeA3() {
		assertThrows(
				IllegalArgumentException.class,
				() -> OrderBy.desc(null)
		);
	}

	@Test
	public void negativeA4() {
		assertThrows(
				IllegalArgumentException.class,
				() -> OrderBy.desc("")
		);
	}

	@Test
	public void negativeB1() {
		assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(TestEnt.class, dataSource).select("some")
						.orderBy(null)
		);
	}

	@Test
	public void negativeB2() {
		assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(TestEnt.class, dataSource).select("some")
						.orderBy(OrderBy.asc("none"))
		);
	}

	@Test
	public void negativeB3() {
		assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(TestEnt.class, dataSource).select("some")
						.groupBy("some")
						.orderBy(OrderBy.asc("id"))
		);
	}

	@Entity
	@Table(name = "some")
	public static class TestEnt {
		@Column
		public String id;
		@Column
		public String some;
	}
}
