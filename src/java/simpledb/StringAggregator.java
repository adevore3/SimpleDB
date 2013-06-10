package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _aop;
    private HashMap<Field, Integer> _count;
    private String _gbfieldname;				 // group by field name
    private String _afieldname;					 // aggregate field name

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	if (what != Aggregator.Op.COUNT) {
    		throw new IllegalArgumentException("Aggregate must be COUNT.");
    	}
    	this._gbfield = gbfield;
        this._gbfieldtype = gbfieldtype;
        this._afield = afield;
        this._aop = what;
        this._count = new HashMap<Field, Integer>();
        this._gbfieldname = null;
        this._afieldname = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	if (this._gbfieldname == null || this._afieldname == null) {
    		if (this._gbfield != Aggregator.NO_GROUPING)
    			this._gbfieldname = tup.getTupleDesc().getFieldName(this._gbfield);
    		
        	this._afieldname = this._aop + "(" + tup.getTupleDesc().getFieldName(this._afield) + ")";
    	}
    	
    	Field key;
        if (this._gbfield == Aggregator.NO_GROUPING) {
        	key = null;
        } else { // group by is specified
        	key = tup.getField(this._gbfield);
        }
        
        if (this._count.containsKey(key)) {
			int oldCount = this._count.get(key);
    		this._count.put(key, oldCount + 1);
		} else {
			this._count.put(key, 1);
		}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		Iterator<Field> keys = this._count.keySet().iterator();
		
		TupleDesc td;// determine _td based on group by or no group by
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
			IntField aggregateResult = new IntField(this._count.get(key));
			
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
