package com.gullerya.sqldsl;

import com.gullerya.sqldsl.api.statements.Select;

import java.util.Map;

public interface Join {

	Map<String, ?> inner(Select.SelectDownstream<?>... joiners);

	Map<String, ?> toward(Select.SelectDownstream<?> primaryEntity, Select.SelectDownstream<?>... joiners);

}
