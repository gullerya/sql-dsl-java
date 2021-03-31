package com.gullerya.sqldsl.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import javax.sql.DataSource;

import com.gullerya.sqldsl.EntityDAL;
import com.gullerya.sqldsl.Literal;
import com.gullerya.sqldsl.api.clauses.Where;

public class EntityDALImpl<ET> implements EntityDAL<ET> {
	private final ESConfig<ET> config;

	public EntityDALImpl(Class<ET> entityType, DataSource ds) throws ReflectiveOperationException {
		EntityMetadata<ET> em = new EntityMetadata<>(entityType);
		this.config = new ESConfig<>(ds, em);
	}

	@Override
	public int delete() {
		return new DeleteImpl<>(config).delete();
	}

	@Override
	public int delete(Where.WhereClause whereClause) {
		return new DeleteImpl<>(config).delete(whereClause);
	}

	@Override
	public boolean insert(ET entity, Literal... literals) {
		return new InsertImpl<>(config).insert(entity, literals);
	}

	@Override
	public int[] insert(Collection<ET> entities, Literal... literals) {
		return new InsertImpl<>(config).insert(entities, literals);
	}

	@Override
	public SelectDownstream<ET> select(String... fields) {
		return new SelectImpl<>(config).select(fields);
	}

	@Override
	public SelectDownstream<ET> select(Set<String> fields) {
		return new SelectImpl<>(config).select(fields);
	}

	@Override
	public UpdateDownstream update(ET entity, Literal... literals) {
		// return new UpdateImpl<>(config).update(entity, literals);
		return null;
	}

	public static final class ESConfig<ET> {
		private final DataSource ds;
		final EntityMetadata<ET> em;

		private ESConfig(DataSource ds, EntityMetadata<ET> em) {
			this.ds = ds;
			this.em = em;
		}

		<R> R prepareStatementAndDo(String sql, PreparedStatementAction<R> psAction) {
			try (Connection c = ds.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
				return psAction.execute(s);
			} catch (SQLException sqle) {
				throw new IllegalStateException(sqle);
			}
		}
	}

	interface PreparedStatementAction<R> {
		R execute(PreparedStatement s) throws SQLException;
	}
}
