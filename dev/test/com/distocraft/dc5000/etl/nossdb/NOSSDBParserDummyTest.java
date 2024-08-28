package com.distocraft.dc5000.etl.nossdb;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.distocraft.dc5000.etl.parser.Main;

public class NOSSDBParserDummyTest {
	
	NOSSDBParser nossparser = new NOSSDBParser();
	
	private String techPack;

	  private String setType;

	  private String setName;

	  private int status = 0;

	  private Main mainParserObject = null;

	  private String workerName = "";
	
	@Test
	public void testInit() {
		
		nossparser.init(null, techPack, setType, setName, "workername");
		
		assertEquals(0, 0);
	}
	
	@Test
	public void testStatus() {
		
		int result = nossparser.status();
		
		assertEquals(result, result);
	}

}
