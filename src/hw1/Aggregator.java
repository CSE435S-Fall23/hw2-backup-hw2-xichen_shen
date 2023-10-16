package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
    private Map<String, Integer> groupMap;
    private AggregateOperator o;
    private Boolean groupBy;
    private TupleDesc td;
    private Map<String, Integer> sumMap;
    private Map<String, Integer> countMap;
    private Map<String, String> stringMaxMap;
    private Map<String, String> stringMinMap;

    public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
        this.groupMap = new HashMap<>();
        this.o = o;
        this.groupBy = groupBy;
        this.td = td;
        this.sumMap = new HashMap<>();
        this.countMap = new HashMap<>();
        this.stringMaxMap = new HashMap<>();
        this.stringMinMap = new HashMap<>();
    }

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
    public void merge(Tuple t) {
        String groupKey = "default";
        int valueIndex;
        if (groupBy) {
            groupKey = t.getField(0).toString();
            valueIndex = 1;
        } else {
            valueIndex = 0;
        }

        if (!groupMap.containsKey(groupKey)) {
            sumMap.put(groupKey, 0);
            countMap.put(groupKey, 0);
            stringMaxMap.put(groupKey, "");
            stringMinMap.put(groupKey, "");
            
            if (o == AggregateOperator.MIN && t.getField(valueIndex).getType() == Type.INT) {
                groupMap.put(groupKey, Integer.MAX_VALUE);
            } else if (o == AggregateOperator.MAX && t.getField(valueIndex).getType() == Type.INT) {
                groupMap.put(groupKey, Integer.MIN_VALUE);
            } else {
                groupMap.put(groupKey, 0);
            }
        }

        if (t.getField(valueIndex).getType() == Type.INT) {
            IntField intField = (IntField) t.getField(valueIndex);
            int value = intField.getValue();
            
            switch (o) {
                case MIN:
                    groupMap.put(groupKey, Math.min(groupMap.get(groupKey), value));
                    break;
                case MAX:
                    groupMap.put(groupKey, Math.max(groupMap.get(groupKey), value));
                    break;
                case COUNT:
                    groupMap.put(groupKey, countMap.get(groupKey) + 1);
                    break;
                case SUM:
                    groupMap.put(groupKey, sumMap.get(groupKey) + value);
                    break;
                case AVG:
                    sumMap.put(groupKey, sumMap.get(groupKey) + value);
                    groupMap.put(groupKey, sumMap.get(groupKey) / (countMap.get(groupKey) + 1));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported operator for int type.");
            }
            
            sumMap.put(groupKey, sumMap.get(groupKey) + value);
            countMap.put(groupKey, countMap.get(groupKey) + 1);

        } else if (t.getField(valueIndex).getType() == Type.STRING) {
            StringField stringField = (StringField) t.getField(valueIndex);
            String value = stringField.getValue();
            
            switch (o) {
                case MIN:
                    String currentMin = stringMinMap.get(groupKey);
                    if (currentMin.isEmpty() || value.compareTo(currentMin) < 0) {
                        stringMinMap.put(groupKey, value);
                    }
                    break;
                case MAX:
                    String currentMax = stringMaxMap.get(groupKey);
                    if (currentMax.isEmpty() || value.compareTo(currentMax) > 0) {
                        stringMaxMap.put(groupKey, value);
                    }
                    break;
                case COUNT:
                    groupMap.put(groupKey, countMap.get(groupKey) + 1);
                    break;
                case SUM:
                case AVG:
                    throw new IllegalArgumentException("SUM/AVG operations not supported on string.");
                default:
                    throw new IllegalArgumentException("Unsupported operator for string type.");
            }
            
            countMap.put(groupKey, countMap.get(groupKey) + 1);
        } else {
            throw new IllegalArgumentException("Unsupported type for aggregation.");
        }
    }
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
    public ArrayList<Tuple> getResults() {
        ArrayList<Tuple> ts = new ArrayList<>();

        groupMap.forEach((s, v) -> {
            if (!groupBy) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(v));
                ts.add(tuple);
            } else {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new StringField(s));
                
                if (o == AggregateOperator.MAX && stringMaxMap.get(s) != null) {
                    tuple.setField(1, new StringField(stringMaxMap.get(s)));
                } else if (o == AggregateOperator.MIN && stringMinMap.get(s) != null) {
                    tuple.setField(1, new StringField(stringMinMap.get(s)));
                } else {
                    tuple.setField(1, new IntField(v));
                }
                
                ts.add(tuple);
            }
        });
        
        return ts;
	}

}
