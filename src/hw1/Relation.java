package hw1;

import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
/**
 * 
 * Student1: Jacob Shen
 * Student2: Xi Chen
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		tuples = l;
        this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		this.tuples = tuples.stream().filter(tuple -> tuple.getField(field).compare(op, operand)).collect(Collectors.toCollection(ArrayList::new));
        return this;
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		String[] newName = new String[td.numFields()];
        Type[] newType = new Type[td.numFields()];
        for (int i = 0; i < td.numFields(); i++) {
            newName[i] = td.getFieldName(i);
            newType[i] = td.getType(i);
        }

        for (int i = 0; i < fields.size(); i++) {
            newName[fields.get(i)] = names.get(i);
        }

        td = new TupleDesc(newType, newName);
        for (Tuple tuple : this.tuples) {
            tuple.setDesc(td);
        }
        return this;
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		Type[] newTypes = new Type[fields.size()];
        String[] newNames = new String[fields.size()];

        for (int i = 0; i < fields.size(); i++) {
            newTypes[i] = td.getType(fields.get(i));
            newNames[i] = td.getFieldName(fields.get(i));
        }

        TupleDesc newTd = new TupleDesc(newTypes, newNames);

        ArrayList<Tuple> newTuples = tuples.stream().map(oldTuple -> {
            Tuple tuple = new Tuple(newTd);
            int index = 0;
            for (int i = 0; i < td.numFields(); i++) {
                if (fields.contains(i)) {
                    tuple.setField(index++, oldTuple.getField(i));
                }
            }
            return tuple;
        }).collect(Collectors.toCollection(ArrayList::new));

        return new Relation(newTuples, newTd);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		String[] newNames = new String[other.td.numFields() + td.numFields()];
        Type[] newTypes = new Type[newNames.length];

        for (int i = 0; i < newNames.length; i++) {
            if (i < td.numFields()) {
                newNames[i] = td.getFieldName(i);
                newTypes[i] = td.getType(i);
            } else {
                newNames[i] = other.td.getFieldName(i - td.numFields());
                newTypes[i] = other.td.getType(i - td.numFields());
            }
        }

        TupleDesc newTd = new TupleDesc(newTypes, newNames);
        ArrayList<Tuple> ntl = new ArrayList<>();

        for (Tuple t1 : tuples) {
            for (Tuple t2 : other.tuples) {
                if (t1.getField(field1).equals(t2.getField(field2))) {
                    Tuple t3 = new Tuple(newTd);
                    for (int i = 0; i < newNames.length; i++) {
                        if (i < td.numFields()) {
                            t3.setField(i, t1.getField(i));
                        } else {
                            t3.setField(i, t2.getField(i - td.numFields()));
                        }
                    }
                    ntl.add(t3);
                }
            }

        }
        
        return new Relation(ntl, newTd);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		Aggregator aggregator = new Aggregator(op, groupBy, td);
        for (Tuple tuple : tuples) {
            aggregator.merge(tuple);
        }
        return new Relation(aggregator.getResults(),td);
	}
	
	public TupleDesc getDesc() {
		//your code here
		return td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		StringBuilder sb = new StringBuilder();
        sb.append(td.toString()).append("\n");
        for (Tuple tuple : tuples) {
            sb.append(tuple.toString()).append("\n");
        }
        return sb.toString();
	}
}
