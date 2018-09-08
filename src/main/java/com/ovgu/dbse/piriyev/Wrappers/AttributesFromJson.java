package com.ovgu.dbse.piriyev.Wrappers;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.ovgu.dbse.piriyev.output.SAttribute;
import com.ovgu.dbse.piriyev.output.SAttributeList;

import db.schema.entity.*;
import db.schema.types.AttributeType;
import db.schema.types.AttributeType.DateAndTimeAttributeType;

public class AttributesFromJson {

	public static List<Attribute> getAttributeList(SAttributeList pojoAttributes) {

		List<Attribute> listOfAttributes = new ArrayList<>();

		if (pojoAttributes.getAttributes().size() > 0) {

			for (SAttribute sAttribute : pojoAttributes.getAttributes()) {

				Attribute attribute = new Attribute(sAttribute.getId(), 
						                            getAttributeType(sAttribute.getType(), sAttribute.getConstraint(), sAttribute.getPrecisionForNumeric()));
			    attribute.primaryKey = Boolean.valueOf(sAttribute.getPrimaryKey().toLowerCase());
			    listOfAttributes.add(attribute);	
			}

		}
		return listOfAttributes;
	}

	public static AttributeType getAttributeType(String sAttrbuteType, String sConstraint, String sForNumeric) {

		switch (sAttrbuteType) {
		case "CHAR":
			return AttributeType.Character(Integer.parseInt(sConstraint));
		case "VARCHAR":
			return AttributeType.CharacterVarying(Integer.parseInt(sConstraint));
		case "TEXT":
			return AttributeType.Text();
		case "SMALLINT":
			return AttributeType.SmallInt();
		case "BIGINT":
			return AttributeType.BigInt();
		case "INTEGER":
			return AttributeType.Integer();
		case "NUMERIC":
			return AttributeType.Numeric(Integer.parseInt(sConstraint), Integer.parseInt(sForNumeric));
		case "REAL":
			return AttributeType.Real();
		case "DOUBLE":
			return AttributeType.Double();
		case "DATE":
			return AttributeType.Date(sConstraint);
		case "TIMESTAMP":
			return AttributeType.Timestamp(sConstraint);
		case "BOOLEAN":
			return AttributeType.Boolean();
		case "BYTEA":
			return AttributeType.Blob();
		default:
			return null;
		}

	}

}
