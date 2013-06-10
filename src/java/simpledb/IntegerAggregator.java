package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _aop;
    private HashMap<Field, Integer> _tuplegroup; // used if there is a group-by field
    private HashMap<Field, Integer> _count;
    private String _gbfieldname;				 // group by field name
    private String _afieldname;					 // aggregate field name

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this._gbfield = gbfield;
        this._gbfieldtype = gbfieldtype;
        this._afield = afield;
        this._aop = what;
        this._tuplegroup = new HashMap<Field, Integer>();
        this._count = new HashMap<Field, Integer>();
        this._gbfieldname = null;
        this._afieldname = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	if (this._gbfieldname == null || this._afieldname == null) {
    		if (this._gbfield != Aggregator.NO_GROUPING)
    			this._gbfieldname = tup.getTupleDesc().getFieldName(this._gbfield);
    		
        	this._afieldname = this._aop + "(" + tup.getTupleDesc().getFieldName(this._afield) + ")";
    	}
    	
    	Field key;
        if (this._gbfield == Aggregator.NO_GROUPING)
        	key = null;
        else // group by is specified
        	key = tup.getField(this._gbfield);
        
        int newValue = ((IntField) tup.getField(this._afield)).getValue();
        switch (this._aop) {
        	case MIN:
        		if (this._tuplegroup.containsKey(key)) {
        			int oldValue = this._tuplegroup.get(key);
            		
        			if (newValue < oldValue) {
            			this._tuplegroup.put(key, newValue);
            		}
        		} else {
        			this._tuplegroup.put(key, newValue);
        		}
        		break;
        	case MAX:
        		if (this._tuplegroup.containsKey(key)) {
        			int oldValue = this._tuplegroup.get(key);
            		
        			if (newValue > oldValue) {
            			this._tuplegroup.put(key, newValue);
            		}
        		} else {
        			this._tuplegroup.put(key, newValue);
        		}
        		break;
        	case SUM:
        		if (this._tuplegroup.containsKey(key)) {
        			int oldValue = this._tuplegroup.get(key);
            		this._tuplegroup.put(key, newValue + oldValue); // store sum of old and new values
        		} else {
        			this._tuplegroup.put(key, newValue);
        		}
        		break;
        	case AVG:
        		if (this._tuplegroup.containsKey(key)) {
        			int oldValue = this._tuplegroup.get(key);
        			int oldCount = this._count.get(key);
        			
        			this._tuplegroup.put(key, newValue + oldValue); // store sum of old and new values
            		this._count.put(key, oldCount + 1);
        		} else {
        			this._tuplegroup.put(key, newValue);
        			this._count.put(key, 1);
        		}
        		break;
        	case COUNT:
        		if (this._tuplegroup.containsKey(key)) {
        			int oldCount = this._tuplegroup.get(key);
            		this._tuplegroup.put(key, oldCount + 1);
        		} else {
        			this._tuplegroup.put(key, 1);
        		}
        		break;
        	default:
        		System.out.println("Unimplemented");
        		break;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		Iterator<Field> keys = this._tuplegroup.keySet().iterator();
		
		TupleDesc td; // determine _td based on group by or no group by
		if (this._gbfield == Aggregator.NO_GROUPING) { // no grouping
			Type[] types = { Type.INT_TYPE };
			String[] fieldAr = { this._afieldname };
			td = new TupleDesc(types, fieldAr);
		} else {
			Type[] types = { this._gbfieldtype, Type.INT_TYPE };
			String[] fieldAr = { this._gbfieldname, this._afieldname };
			td = new TupleDesc(types, fieldAr);
		}
		
		while (keys.hasNext()) { // should only have one key if no grouping
			Field key = keys.next();
			IntField aggregateResult;
			
			switch (this._aop) {
    			case MIN: // min, max, sum are all the same
    			case MAX:
    			case SUM:
    				aggregateResult = new IntField(this._tuplegroup.get(key));
    				break;
    			case AVG: // need the result of tupleGroup and count
    				int value = this._tuplegroup.get(key) / this._count.get(key);
    				aggregateResult = new IntField(value);
    				break;
    			case COUNT: // only needs count
    				aggregateResult = new IntField(this._tuplegroup.get(key));
    				break;
    			default: // unimplemented cases
    				System.out.println("Unimplemented");
    				aggregateResult = null;
    				break;
			}
			
    		Tuple result = new Tuple(td); // determine result based on group by or no group by
    		if (this._gbfield == Aggregator.NO_GROUPING) { // no grouping
    			result.setField(0, aggregateResult);
    		} else {
    			result.setField(0, key);
    			result.setField(1, aggregateResult);
    		}
			
			tuples.add(result);
		}
		
		// special case of no tuples to aggregate over
		if (tuples.size() == 0 && this._gbfield == Aggregator.NO_GROUPING) {
			Tuple result = new Tuple(td);
			IntField aggregateResult = (this._aop == Aggregator.Op.COUNT) ? new IntField(0) : null;
			
			result.setField(0, aggregateResult);
			tuples.add(result);
		}
    
        return new TupleIterator(td, tuples);
    }

}
