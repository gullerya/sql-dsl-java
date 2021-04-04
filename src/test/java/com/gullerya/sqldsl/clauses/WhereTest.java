package com.gullerya.sqldsl.clauses;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

import com.gullerya.sqldsl.DBUtils;
import com.gullerya.sqldsl.EntityDAL;
import com.gullerya.sqldsl.api.clauses.Where;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class WhereTest {
	private final DataSource dataSource = DBUtils.getDataSource();

	@Test
	public void negative1() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Where.eq(null, null)
		);
	}

	@Test
	public void negative2() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Where.eq("", null)
		);
	}

	@Test
	public void negative3() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Where.isNull(null)
		);
	}

	@Test
	public void negative4() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Where.isNotNull("")
		);
	}

	@Test
	public void negative5() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Where.eq("some", null)
		);
	}

	@Test
	public void negative6() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Where.not(null)
		);
	}

	@Test
	public void negativeA1() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Where.between("some", null, null)
		);
	}

	@Test
	public void negativeA2() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Where.between("some", 5, null)
		);
	}

	@Test
	public void negativeB1() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Where.in("some", (Object[]) null)
		);
	}

	@Test
	public void negativeB2() {
		assertThrows(
				IllegalArgumentException.class,
				() -> {
					Object[] vs = new Object[0];
					Where.in("some", vs);
				}
		);
	}

	@Test
	public void negativeB3() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Where.in("some", (Collection<Object>) null)
		);
	}

	@Test
	public void negativeB4() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Where.in("some", new ArrayList<>())
		);
	}

	@Test
	public void negativeC4() {
		assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(TestEnt.class, dataSource)
						.select("some")
						.where(null)
		);
	}

	@Test
	public void negativeC5() {
		assertThrows(
				IllegalArgumentException.class,
				() -> EntityDAL.of(TestEnt.class, dataSource)
						.select("some")
						.where(Where.eq("none", 6))
		);
	}

	@Entity
	@Table(name = "some")
	public static class TestEnt {
		@Column
		public String some;
	}
}
