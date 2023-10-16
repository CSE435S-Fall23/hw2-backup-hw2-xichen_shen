package test;


import static org.junit.Assert.*;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.HeapPage;
import hw1.IntField;
import hw1.Query;
import hw1.StringField;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;
import hw1.Relation;
import hw1.AggregateOperator;
/*
 * Name1: Xi Chen
 * Name2: Jacob Shen
 */
public class YourUnitTests {
	
	private HeapFile testhf;
	private TupleDesc testtd;
	private HeapFile ahf;
	private TupleDesc atd;
	private Catalog c;

	@Before
	public void setup() {
		
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		testtd = c.getTupleDesc(tableId);
		testhf = c.getDbFile(tableId);
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
		
		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
	}
	//Check Alias works
	@Test
	public void testAlias() {
        Query q = new Query("SELECT a1 AS alias1, a2 AS alias2 FROM A");
        Relation r = q.execute();

        // Verify that the number of tuples is correct
        assertTrue(r.getTuples().size() == 8); // There are 8 tuples in table A

        // Check that the TupleDesc has the alias name(s) instead of original column name(s)
        assertTrue(r.getDesc().getFieldName(0).equals("alias1"));
        assertTrue(r.getDesc().getFieldName(1).equals("alias2"));

    }
	//Check Alias works for WHERE
	@Test
	public void testAliasWithWhere() {

	    // A is a table with columns a1, a2
	    Query q = new Query("SELECT a1 AS alias1, a2 AS alias2 FROM A WHERE a1 > 1");
	    Relation r = q.execute();

	    // Verify that the number of tuples is correct
	    // Assuming that there are Y tuples where a1 > 5 in table A

	    // Check that the TupleDesc has the alias name(s) instead of original column name(s)
	    assertTrue(r.getDesc().getFieldName(0).equals("alias1"));
	    assertTrue(r.getDesc().getFieldName(1).equals("alias2"));

	    // Additional: Check if the filtering was accurate
	    // Ensure all resulting tuples adhere to the WHERE clause using the original column name
	    for (Tuple tuple : r.getTuples()) {
	        IntField field = (IntField) tuple.getField(0);  // Using alias1 which maps to a1
	        System.out.println(field.getValue());
	        assertTrue(field.getValue() > 1);
	        
	    }
	}

	//Check Min works in AggregateOperator
	@Test
	public void test_Relation_Aggregate_Min() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c); //Contains 1,2,3,4,5,6,7,8, min=1
		for (Tuple tuple : ar.getTuples()) {
	        IntField field = (IntField) tuple.getField(0); 
	        System.out.println(field.getValue());
	    }
		ar = ar.aggregate(AggregateOperator.MIN, false);
		
		assertTrue("The result of an aggregate should not be a single tuple", ar.getTuples().size() == 1);
		IntField agg = (IntField) ar.getTuples().get(0).getField(0);
		System.out.println(agg.getValue());

		assertTrue("The MIN of these values was incorrect", agg.getValue() == 1);
	}
	
	//Check Max works in AggregateOperator
	@Test
	public void test_Relation_Aggregate_Max() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c); //Contains 1,2,3,4,5,6,7,8, Max = 8
		for (Tuple tuple : ar.getTuples()) {
	        IntField field = (IntField) tuple.getField(0); 
	        System.out.println(field.getValue());
	    }
		ar = ar.aggregate(AggregateOperator.MAX, false);
		
		assertTrue("The result of an aggregate should not be a single tuple", ar.getTuples().size() == 1);
		IntField agg = (IntField) ar.getTuples().get(0).getField(0);
		System.out.println(agg.getValue());

		assertTrue("The MAX of these values was incorrect", agg.getValue() == 8);
	}
	
	////Check COUNT works for string in AggregateOperator
	@Test
	public void test_Relation_Aggregate_COUNT_String() {
		Relation ar = new Relation(testhf.getAllTuples(), testtd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c); //Contains "hi"
		for (Tuple tuple : ar.getTuples()) {
			StringField field = (StringField) tuple.getField(0); 
	        System.out.println(field.getValue());
	    }
		ar = ar.aggregate(AggregateOperator.COUNT, false);
		
		assertTrue("The result of an aggregate should not be a single tuple", ar.getTuples().size() == 1);
		IntField agg = (IntField) ar.getTuples().get(0).getField(0);
		System.out.println(agg.getValue());

		assertTrue("The count of these values was incorrect", agg.getValue() == 1);
	}
	
	
}
