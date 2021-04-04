package com.gullerya.sqldsl;

import com.gullerya.sqldsl.api.clauses.OrderBy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static com.gullerya.sqldsl.api.clauses.Where.*;
import static org.junit.jupiter.api.Assertions.*;

public class LiteralsTest {
	private static final String TABLE_NAME = "LiteralsTestTable";
	private static final DataSource dataSource = DBUtils.getDataSource();
	private static final EntityDAL<LitTestEnt> es = EntityDAL.of(LitTestEnt.class, dataSource);

	@BeforeAll
	public static void prepare() throws Exception {
		dataSource.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS \"" + TABLE_NAME + "\";" +
								"CREATE TABLE \"" + TABLE_NAME + "\" (" +
								"id         BIGINT DEFAULT 0," +
								"price      DECIMAL(15,4)," +
								"updated    TIMESTAMP" +
								")"
				)
				.execute();
	}

	@Test
	public void insertOne() {
		LitTestEnt e = new LitTestEnt(1000L, BigDecimal.valueOf(2.4), null);
		int r = es.insert(e, Literal.exp("id", "DEFAULT"), Literal.exp("updated", "TIMEZONE('UTC', CURRENT_TIMESTAMP)"));
		assertEquals(1, r);

		LitTestEnt re = es.select("id", "price", "updated").where(eq("price", 2.4)).readSingle();
		assertNotNull(re);
		assertEquals(0, (long) re.id);
		assertEquals(2.4, re.price.doubleValue(), 0);
		assertNotNull(re.updated);
	}

	@Test
	public void insertMany() {
		List<LitTestEnt> el = new ArrayList<>();
		el.add(new LitTestEnt(2000L, BigDecimal.valueOf(2.4), LocalDateTime.now()));
		el.add(new LitTestEnt(2001L, BigDecimal.valueOf(2.4), null));
		int[] is = es.insert(el, Literal.exp("price", "NULL"));
		assertTrue(IntStream.of(is).allMatch(i -> i == 1));

		List<LitTestEnt> re = es
				.select("id", "price", "updated")
				.where(and(Arrays.asList(gte("id", 2000), lte("id", 2001))))
				.orderBy(OrderBy.asc("updated"))
				.read();
		assertNotNull(re);
		assertEquals(2, re.size());
		assertEquals(2000, (long) re.get(0).id);
		assertNull(re.get(0).price);
		assertNotNull(re.get(0).updated);
		assertEquals(2001, (long) re.get(1).id);
		assertNull(re.get(1).price);
		assertNull(re.get(1).updated);
	}

	@Test
	public void update() {
		List<LitTestEnt> el = new ArrayList<>();
		el.add(new LitTestEnt(3000L, BigDecimal.valueOf(0), LocalDateTime.now()));
		el.add(new LitTestEnt(3001L, BigDecimal.valueOf(1), null));
		el.add(new LitTestEnt(3002L, BigDecimal.valueOf(2), null));
		int[] is = es.insert(el, Literal.exp("price", "NULL"));
		assertTrue(IntStream.of(is).allMatch(i -> i == 1));

		int ur = es.update(new LitTestEnt(), Literal.exp("price", "DEFAULT"), Literal.exp("updated", "CURRENT_TIMESTAMP"))
				.where(between("id", 3000, 3002));
		assertEquals(3, ur);

		List<LitTestEnt> re = es.select("price", "updated").where(between("id", 3000, 3002)).read();
		assertNotNull(re);
		assertEquals(3, re.size());
		assertNull(re.get(0).price);
		assertNotNull(re.get(0).updated);
		assertNull(re.get(1).price);
		assertEquals(re.get(0).updated, re.get(1).updated);
		assertNull(re.get(2).price);
		assertEquals(re.get(0).updated, re.get(2).updated);
	}

	@Test
	public void negativeA1() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Literal.exp(null, null)
		);
	}

	@Test
	public void negativeA2() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Literal.exp("", null)
		);
	}

	@Test
	public void negativeA3() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Literal.exp("some", null)
		);
	}

	@Test
	public void negativeA4() {
		assertThrows(
				IllegalArgumentException.class,
				() -> Literal.exp("some", "")
		);
	}

	@Test
	public void negativeB1() {
		assertThrows(
				IllegalArgumentException.class,
				() -> es.insert(new LitTestEnt(), Literal.exp("non-existing", "DEFAULT"), Literal.exp("updated", "CURRENT_TIMESTAMP"))
		);
	}

	@Test
	public void negativeB2() {
		assertThrows(
				IllegalArgumentException.class,
				() -> es.update(new LitTestEnt(), Literal.exp("price", "DEFAULT"), Literal.exp("non-existing", "CURRENT_TIMESTAMP")).all()
		);

	}

	@Entity
	@Table(name = TABLE_NAME)
	public static final class LitTestEnt {
		@Column
		public Long id;
		@Column
		public BigDecimal price;
		@Column
		public LocalDateTime updated;

		public LitTestEnt() {
		}

		LitTestEnt(Long id, BigDecimal price, LocalDateTime updated) {
			this.id = id;
			this.price = price;
			this.updated = updated;
		}
	}
}
