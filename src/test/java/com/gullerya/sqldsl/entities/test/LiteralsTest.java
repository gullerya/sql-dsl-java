// package com.gullerya.sqldsl.entities.test;

// import com.gullerya.sql._configuration.DataSourceDetails;
// import com.gullerya.sql._configuration.DataSourceProvider;
// import com.gullerya.sql._configuration.DataSourceProviderSPI;
// import com.gullerya.sql.entities.EntityField;
// import com.gullerya.sql.entities.EntityService;
// import com.gullerya.sql.entities.Literal;
// import com.gullerya.sql.entities.OrderBy;
// import com.gullerya.sqldsl.DBUtils;

// import org.junit.Assert;
// import org.junit.BeforeClass;
// import org.junit.Test;

// import javax.persistence.Entity;
// import javax.persistence.Table;
// import javax.sql.DataSource;

// import static com.gullerya.sql.entities.Where.and;
// import static com.gullerya.sql.entities.Where.between;
// import static com.gullerya.sql.entities.Where.eq;
// import static com.gullerya.sql.entities.Where.gte;
// import static com.gullerya.sql.entities.Where.lte;

// import java.math.BigDecimal;
// import java.time.LocalDateTime;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.List;
// import java.util.stream.IntStream;

// public class LiteralsTest {
// 	private static final String TABLE_NAME = "LiteralsTestTable";
// 	private static final DataSourceProviderSPI config = new DataSourceProviderSPI() {
// 		@Override
// 		public String getDBConfigLocation() {
// 			return "test.db.properties";
// 		}
// 	};
// 	private static final DataSourceDetails dsd = DataSourceProvider.getInstance(config).getDataSourceDetails();
// 	private static final DataSource dataSource = dsd.getDataSource();
// 	private static final EntityDAL<LitTestEnt> es = EntityDAL.of(LitTestEnt.class, dataSource);

// 	@BeforeClass
// 	public static void prepare() throws Exception {
// 		DBUtils.ensureSchema(dsd);
// 		dataSource.getConnection()
// 				.prepareStatement(
// 						"DROP TABLE IF EXISTS \"" + DBUtils.DAL_TESTS_SCHEMA + "\".\"" + TABLE_NAME + "\";" +
// 								"CREATE TABLE \"" + DBUtils.DAL_TESTS_SCHEMA + "\".\"" + TABLE_NAME + "\" (" +
// 								"id         BIGINT DEFAULT 0," +
// 								"price      DECIMAL(15,4)," +
// 								"updated    TIMESTAMP" +
// 								");"
// 				)
// 				.execute();
// 	}

// 	@Test
// 	public void insertOne() {
// 		LitTestEnt e = new LitTestEnt(1000L, BigDecimal.valueOf(2.4), null);
// 		boolean is = es.insert(e, Literal.exp("id", "DEFAULT"), Literal.exp("updated", "TIMEZONE('UTC', CURRENT_TIMESTAMP)"));
// 		Assert.assertTrue(is);

// 		LitTestEnt re = es.select("id", "price", "updated").where(eq("price", 2.4)).readSingle();
// 		Assert.assertNotNull(re);
// 		Assert.assertEquals(0, (long) re.id);
// 		Assert.assertEquals(2.4, re.price.doubleValue(), 0);
// 		Assert.assertNotNull(re.updated);
// 	}

// 	@Test
// 	public void insertMany() {
// 		List<LitTestEnt> el = new ArrayList<>();
// 		el.add(new LitTestEnt(2000L, BigDecimal.valueOf(2.4), LocalDateTime.now()));
// 		el.add(new LitTestEnt(2001L, BigDecimal.valueOf(2.4), null));
// 		int[] is = es.insert(el, Literal.exp("price", "NULL"));
// 		Assert.assertTrue(IntStream.of(is).allMatch(i -> i == 1));

// 		List<LitTestEnt> re = es
// 				.select("id", "price", "updated")
// 				.where(and(Arrays.asList(gte("id", 2000), lte("id", 2001))))
// 				.orderBy(OrderBy.asc("updated"))
// 				.read();
// 		Assert.assertNotNull(re);
// 		Assert.assertEquals(2, re.size());
// 		Assert.assertEquals(2000, (long) re.get(0).id);
// 		Assert.assertNull(re.get(0).price);
// 		Assert.assertNotNull(re.get(0).updated);
// 		Assert.assertEquals(2001, (long) re.get(1).id);
// 		Assert.assertNull(re.get(1).price);
// 		Assert.assertNull(re.get(1).updated);
// 	}

// 	@Test
// 	public void update() {
// 		List<LitTestEnt> el = new ArrayList<>();
// 		el.add(new LitTestEnt(3000L, BigDecimal.valueOf(0), LocalDateTime.now()));
// 		el.add(new LitTestEnt(3001L, BigDecimal.valueOf(1), null));
// 		el.add(new LitTestEnt(3002L, BigDecimal.valueOf(2), null));
// 		int[] is = es.insert(el, Literal.exp("price", "NULL"));
// 		Assert.assertTrue(IntStream.of(is).allMatch(i -> i == 1));

// 		int ur = es.update(new LitTestEnt(), Literal.exp("price", "DEFAULT"), Literal.exp("updated", "CURRENT_TIMESTAMP"))
// 				.where(between("id", 3000, 3002));
// 		Assert.assertEquals(3, ur);

// 		List<LitTestEnt> re = es.select("price", "updated").where(between("id", 3000, 3002)).read();
// 		Assert.assertNotNull(re);
// 		Assert.assertEquals(3, re.size());
// 		Assert.assertNull(re.get(0).price);
// 		Assert.assertNotNull(re.get(0).updated);
// 		Assert.assertNull(re.get(1).price);
// 		Assert.assertEquals(re.get(0).updated, re.get(1).updated);
// 		Assert.assertNull(re.get(2).price);
// 		Assert.assertEquals(re.get(0).updated, re.get(2).updated);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negativeA1() {
// 		Literal.exp(null, null);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negativeA2() {
// 		Literal.exp("", null);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negativeA3() {
// 		Literal.exp("some", null);
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negativeA4() {
// 		Literal.exp("some", "");
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negativeB1() {
// 		es.insert(new LitTestEnt(), Literal.exp("non-existing", "DEFAULT"), Literal.exp("updated", "CURRENT_TIMESTAMP"));
// 	}

// 	@Test(expected = IllegalArgumentException.class)
// 	public void negativeB2() {
// 		es.update(new LitTestEnt(), Literal.exp("price", "DEFAULT"), Literal.exp("non-existing", "CURRENT_TIMESTAMP")).all();
// 	}

// 	@Entity
// 	@Table(name = TABLE_NAME, schema = DBUtils.DAL_TESTS_SCHEMA)
// 	public static final class LitTestEnt {
// 		@EntityField("id")
// 		public Long id;
// 		@EntityField("price")
// 		public BigDecimal price;
// 		@EntityField("updated")
// 		public LocalDateTime updated;

// 		public LitTestEnt() {
// 		}

// 		LitTestEnt(Long id, BigDecimal price, LocalDateTime updated) {
// 			this.id = id;
// 			this.price = price;
// 			this.updated = updated;
// 		}
// 	}
// }
