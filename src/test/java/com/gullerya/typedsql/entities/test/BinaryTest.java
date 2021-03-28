package com.gullerya.typedsql.entities.test;

import com.gullerya.typedsql.DBUtils;
import com.gullerya.typedsql.configuration.DataSourceDetails;
import com.gullerya.typedsql.configuration.DataSourceProvider;
import com.gullerya.typedsql.configuration.DataSourceProviderSPI;
import com.gullerya.typedsql.entities.EntityService;
import com.gullerya.typedsql.entities.EntityField;
import com.gullerya.typedsql.entities.Where;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BinaryTest {
	private static final String TABLE_NAME = "BinaryTestTable";
	private static final DataSourceProviderSPI config = new DataSourceProviderSPI() {
		@Override
		public String getDBConfigLocation() {
			return "test.db.properties";
		}
	};
	private static final DataSourceDetails dsd = DataSourceProvider.getInstance(config).getDataSourceDetails();
	private static final DataSource dataSource = dsd.getDataSource();
	private static final EntityService<BinaryContainer> binConService = EntityService.of(BinaryContainer.class, dataSource);

	@BeforeClass
	public static void prepare() throws Exception {
		DBUtils.ensureSchema(dsd);
		dataSource.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS \"" + DBUtils.DAL_TESTS_SCHEMA + "\".\"" + TABLE_NAME + "\";" +
								"CREATE TABLE \"" + DBUtils.DAL_TESTS_SCHEMA + "\".\"" + TABLE_NAME + "\" (" +
								"   id      BIGINT," +
								"   barray  BYTEA," +
								"   stream  BYTEA" +
								");"
				)
				.execute();
	}

	@Test
	public void createSingle() {
		binConService.delete();

		//  insert single
		byte[] av = new byte[]{1, 2, 3, 4, 5, 6, 7};
		byte[] sv = new byte[]{6, 5, 4, 3, 2, 1};
		BinaryContainer bc = new BinaryContainer();
		bc.id = 1000L;
		bc.barray = av;
		bc.stream = new ByteArrayInputStream(sv);
		Assert.assertTrue(binConService.insert(bc));

		//  read single and verify
		BinaryContainer br = binConService.select("barray", "stream").where(Where.eq("id", 1000)).readSingle();
		Assert.assertNotNull(br);
		Assert.assertArrayEquals(av, br.barray);
		Assert.assertArrayEquals(sv, isToBytes(br.stream));
	}

	@Test
	public void updateSingle() {
		binConService.delete();

		//  insert single
		byte[] av1 = new byte[]{1, 2, 3, 4, 5, 6, 7};
		byte[] sv1 = new byte[]{6, 5, 4, 3, 2, 1};
		BinaryContainer bc = new BinaryContainer();
		bc.id = 2000L;
		bc.barray = av1;
		bc.stream = new ByteArrayInputStream(sv1);
		Assert.assertTrue(binConService.insert(bc));

		//  update single
		byte[] av2 = new byte[]{2, 3, 4, 5, 6};
		byte[] sv2 = new byte[]{9, 8, 7, 6, 5, 4, 3, 2, 1};
		BinaryContainer bu = new BinaryContainer();
		bu.barray = av2;
		bu.stream = new ByteArrayInputStream(sv2);
		Assert.assertEquals(1, (int) binConService.update(bu).where(Where.eq("id", 2000)));

		//  read single and verify
		BinaryContainer br = binConService.select("barray", "stream").where(Where.eq("id", 2000)).readSingle();
		Assert.assertNotNull(br);
		Assert.assertArrayEquals(av2, br.barray);
		Assert.assertArrayEquals(sv2, isToBytes(br.stream));
	}

	@Test
	public void createSeveral() {
		binConService.delete();

		//  insert several
		List<BinaryContainer> csl = new ArrayList<>();
		BinaryContainer bc = new BinaryContainer();
		bc.id = 3000L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		bc = new BinaryContainer();
		bc.id = 3001L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		bc = new BinaryContainer();
		bc.id = 3002L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		Assert.assertArrayEquals(new int[]{1, 1, 1}, binConService.insert(csl));

		//  read several and verify
		List<BinaryContainer> brs = binConService.select("id", "barray", "stream").read();
		Assert.assertNotNull(brs);
		Assert.assertEquals(3, brs.size());
		for (BinaryContainer br : brs) {
			Assert.assertNotNull(br.id);
			Assert.assertArrayEquals(ByteUtils.longToBytes(br.id), br.barray);
			Assert.assertArrayEquals(ByteUtils.longToBytes(br.id), isToBytes(br.stream));
		}
	}

	@Test
	public void updateSeveral() {
		binConService.delete();

		//  insert several
		List<BinaryContainer> csl = new ArrayList<>();
		BinaryContainer bc = new BinaryContainer();
		bc.id = 4000L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		bc = new BinaryContainer();
		bc.id = 4001L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		bc = new BinaryContainer();
		bc.id = 4002L;
		bc.barray = ByteUtils.longToBytes(bc.id);
		bc.stream = new ByteArrayInputStream(bc.barray);
		csl.add(bc);
		Assert.assertArrayEquals(new int[]{1, 1, 1}, binConService.insert(csl));

		//  update several
		BinaryContainer bu = new BinaryContainer();
		bu.barray = new byte[]{0, 0};
		Assert.assertEquals(3, binConService.update(bu).all());

		//  read several and verify
		List<BinaryContainer> brs = binConService.select("id", "barray", "stream").read();
		Assert.assertNotNull(brs);
		Assert.assertEquals(3, brs.size());
		for (BinaryContainer br : brs) {
			Assert.assertNotNull(br.id);
			Assert.assertArrayEquals(new byte[]{0, 0}, br.barray);
			Assert.assertArrayEquals(ByteUtils.longToBytes(br.id), isToBytes(br.stream));
		}
	}

	@Entity
	@Table(name = TABLE_NAME, schema = DBUtils.DAL_TESTS_SCHEMA)
	public static final class BinaryContainer {
		@EntityField(value = "id", readonly = true)
		public Long id;
		@EntityField("barray")
		public byte[] barray;
		@EntityField("stream")
		public InputStream stream;
	}

	private byte[] isToBytes(InputStream is) {
		if (is == null) {
			throw new IllegalArgumentException("input stream MUST NOT be NULL");
		}

		byte[] buffer = new byte[4096];
		int length;
		try (ByteArrayOutputStream result = new ByteArrayOutputStream()) {
			while ((length = is.read(buffer)) != -1) {
				result.write(buffer, 0, length);
			}
			return result.toByteArray();
		} catch (IOException ioe) {
			throw new RuntimeException("failed to read input stream to bytes", ioe);
		}
	}

	private static final class ByteUtils {
		private static final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

		public static byte[] longToBytes(long x) {
			buffer.putLong(0, x);
			return buffer.array().clone();
		}

		public static long bytesToLong(byte[] bytes) {
			buffer.put(bytes, 0, bytes.length);
			buffer.flip();
			return buffer.getLong();
		}
	}
}
