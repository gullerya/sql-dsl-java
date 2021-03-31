// package com.gullerya.sqldsl.impl;

// import java.util.ArrayList;
// import java.util.Collection;

// import com.gullerya.sqldsl.EntityDAL;
// import com.gullerya.sqldsl.api.statements.Delete;

// public class DeleteImpl<T> implements Delete<T> {
// 	private final EntityDAL.ESConfig<T> config;

// 	DeleteImpl(EntityDAL.ESConfig<T> config) {
// 		this.config = config;
// 	}

// 	@Override
// 	public int delete() {
// 		return internalDelete(null);
// 	}

// 	@Override
// 	public int delete(Where.WhereClause where) {
// 		Where.validate(config.em, where);
// 		return internalDelete(where);
// 	}

// 	private int internalDelete(Where.WhereClause where) {
// 		String sql = "DELETE FROM " + config.em.fqSchemaTableName;
// 		Collection<Where.WhereFieldValuePair> parametersCollector = new ArrayList<>();
// 		if (where != null) {
// 			sql += " WHERE " + where.stringify(parametersCollector);
// 		}
// 		return config.preparedStatementAndDo(sql, s -> {
// 			if (where != null) {
// 				int i = 0;
// 				for (Where.WhereFieldValuePair parameter : parametersCollector) {
// 					i++;
// 					EntityDAL.FieldMetadata fm = config.em.byColumn.get(parameter.column);
// 					if (fm.jdbcConverter != null) {
// 						fm.jdbcConverter.toDB(s, i, parameter.value);
// 					} else {
// 						s.setObject(i, fm.typeConverter.toDB(parameter.value));
// 					}
// 				}
// 			}
// 			return s.executeUpdate();
// 		});
// 	}
// }
