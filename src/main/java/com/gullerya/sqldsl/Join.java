package com.gullerya.sqldsl;

import com.gullerya.sqldsl.api.statements.Select;

import java.util.Map;

public interface Join {

	static Map<String, ?> inner(Select.SelectDownstream<?>... joiners) {
		return null;
	}

	static Map<String, ?> toward(Select.SelectDownstream<?> primaryEntity, Select.SelectDownstream<?>... joiners) {
		return null;
	}

}
