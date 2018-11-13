package com.ovgu.dbse.piriyev.output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SAttribute {
	
	String id;
	
	String type;
	
	String primaryKey;
	
	String constraint;
	String precisionForNumeric;

	public SAttribute(String constructorString) {
		JsonParser parser = new JsonParser();
		Gson gson = new Gson();
		JsonObject asObject = parser.parse(constructorString).getAsJsonObject();
		this.id=asObject.get("id").getAsString();
		this.type=asObject.get("type").getAsString();
		this.primaryKey= asObject.get("primaryKey").getAsString();
		this.constraint = asObject.get("constraint").getAsString();
		this.precisionForNumeric = asObject.get("precisionForNumeric").getAsString();
	}
	
	@JsonProperty
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@JsonProperty
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@JsonProperty
	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	@JsonProperty
	public String getConstraint() {
		return constraint;
	}

	public void setConstraint(String constraint) {
		this.constraint = constraint;
	}

	@JsonProperty
	public String getPrecisionForNumeric() {
		return precisionForNumeric;
	}

	public void setPrecisionForNumeric(String precisionForNumeric) {
		this.precisionForNumeric = precisionForNumeric;
	}
    

}
