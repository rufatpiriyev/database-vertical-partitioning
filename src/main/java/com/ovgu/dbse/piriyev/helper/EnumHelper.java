package com.ovgu.dbse.piriyev.helper;

public enum EnumHelper {

	AUTOPART("AUTOPART"), HILLCLIMB("HILLCLIMB"), HYRISE("HYRISE"), NAVATHE("NAVATHE"), O2P("O2P"), TROJAN(
			"TROJAN"), OPTIMAL("OPTIMAL"), DREAM("DREAM"), COLUMN("COLUMN"), ROW("ROW");

	private String text;


    EnumHelper(String text) {
        this.text = text;
    }

    public String text() {
        return text;
    }
}
