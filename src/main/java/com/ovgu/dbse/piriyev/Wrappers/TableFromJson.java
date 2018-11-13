package com.ovgu.dbse.piriyev.Wrappers;

import java.util.List;

import com.ovgu.dbse.piriyev.output.STable;

import db.schema.entity.Attribute;
import db.schema.entity.Table;
import db.schema.entity.Workload;
import db.schema.types.AttributeType;
import db.schema.types.TableType;

public class TableFromJson {
	
	public static Table getTableFromJson(STable stable, List<Attribute> attributes, Workload wkld) {
		Table table = new Table(stable.getName(), getTableType(stable.getType()), attributes);
		table.workload = wkld;
		return table;	
	}
	
	public static TableType getTableType(String tableType) {

		switch (tableType.toUpperCase()) {
		case "DEFAULT":
			return TableType.Default();
		case "STREAMTABLE":
			return TableType.Stream();
		case "CTABLE":
			return TableType.ColumnGrouped();
		default:
			return null;
		}
		
	}
	

}
