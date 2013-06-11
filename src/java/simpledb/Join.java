package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    
    private JoinPredicate _p;
    private DbIterator _child1;
    private DbIterator _child2;
    private TupleDesc _joinTupleDesc;
    
    private HashJoin _hj;
    
	/**
	 * Private inner class that helps with joining the two tables
	 * using a hash join implementation.
	 * 
	 * @author Anton
	 *
	 */
	private class HashJoin implements Serializable {
	    
	    private static final long serialVersionUID = 1L;
		
		private HashMap<Field, ArrayList<Tuple>> _r1;
		private HashMap<Field, ArrayList<Tuple>> _r2;
		private ArrayList<Field> _keys1;
		private ArrayList<Field> _keys2;
		private int _key1Index;
		private int _key2Index;
		private int _r1Index;
		private int _r2Index;
		
		private DbIterator _child1;
		private DbIterator _child2;
		private JoinPredicate _jp;
		private boolean _tuplesHashed;
		
		public HashJoin() {
			this._r1 = null;
			this._r2 = null;
			this._keys1 = null;
			this._keys2 = null;
			this._key1Index = 0;
			this._key2Index = 0;
			this._r1Index = 0;
			this._r2Index = 0;
			this._child1 = null;
			this._child2 = null;
			this._jp = null;
			this._tuplesHashed = false;
		}
		
		public void open(DbIterator child1, DbIterator child2, JoinPredicate jp) throws NoSuchElementException, DbException, TransactionAbortedException {
			this._child1 = child1;
			this._child2 = child2;
			this._jp = jp;
		}
		
		public void calculateHashTables() throws NoSuchElementException, DbException, TransactionAbortedException {
			if (this._child1.hasNext() && this._child2.hasNext()) {
				if (this._r1 == null && this._r2 == null) {
					this._r1 = new HashMap<Field, ArrayList<Tuple>>();
					this._r2 = new HashMap<Field, ArrayList<Tuple>>();
					
			    	while (this._child1.hasNext()) { // hash all of child1's tuples
			    		Tuple next = this._child1.next();
			    		Field key = next.getField(this._jp.getField1());
			    		
			    		if (this._r1.containsKey(key)) {
			    			this._r1.get(key).add(next);
			    		} else {
			    			ArrayList<Tuple> bucket = new ArrayList<Tuple>();
			    			bucket.add(next);
			    			this._r1.put(key, bucket);
			    		}
			    	}
			    	
			    	while (this._child2.hasNext()) { // hash all of child2's tuples
			    		Tuple next = this._child2.next();
			    		Field key = next.getField(this._jp.getField2());
			    		
			    		if (this._r2.containsKey(key)) {
			    			this._r2.get(key).add(next);
			    		} else {
			    			ArrayList<Tuple> bucket = new ArrayList<Tuple>();
			    			bucket.add(next);
			    			this._r2.put(key, bucket);
			    		}
			    	}
			    	
			    	if (this._jp.getOperator() == Predicate.Op.EQUALS) {
			    		this._r1.keySet().retainAll(this._r2.keySet());
			    		this._r2.keySet().retainAll(this._r1.keySet());
			    	}
			    	
			    	// save the keys for each relation, using a treeset orders keys
			    	this._keys1 = new ArrayList<Field>(new TreeSet<Field>(this._r1.keySet()));
			    	this._keys2 = new ArrayList<Field>(new TreeSet<Field>(this._r2.keySet()));
				}
			}
		}
		
		public void close() {
			this._r1 = null;
			this._r2 = null;
			this._keys1 = null;
			this._keys2 = null;
			this._key1Index = 0;
			this._key2Index = 0;
			this._r1Index = 0;
			this._r2Index = 0;
		}
		
		public void rewind() {
			this._key1Index = 0;
			this._key2Index = 0;
			this._r1Index = 0;
			this._r2Index = 0;
		}
		
		public boolean hasNext() throws DbException, TransactionAbortedException {
			if (this._child1 != null || this._child2 != null || this._jp != null) // checks if joined has been opened
				if (! this._tuplesHashed) // checks if tuples have been hashed
					try {
						this.calculateHashTables();
						this._tuplesHashed = true;
					} catch (NoSuchElementException e) { // problem hashing tuples or no tuples to hash
						return false;
					}
			
			if (this._r1 == null || this._r2 == null // checks if join has been opened
					|| this._key1Index == this._keys1.size()
					|| this._key2Index == this._keys2.size()) //  or if all tuples have been joined
				return false;
			
			Field key1 = this._keys1.get(this._key1Index);
			Field key2 = this._keys2.get(this._key2Index);
			
			return  this._r1Index < this._r1.get(key1).size() || this._r2Index < this._r2.get(key2).size();
		}
		
		public Tuple next() throws DbException, TransactionAbortedException {
			while (this.hasNext()) {
				Field key1 = this._keys1.get(this._key1Index); // grab keys
				Field key2 = this._keys2.get(this._key2Index);
				
				Tuple t1 = this._r1.get(key1).get(this._r1Index); // grab tuples
	    		Tuple t2 = this._r2.get(key2).get(this._r2Index);
	    		
	    		Tuple joinTuple = null;
	    		
	    		if (this._jp.filter(t1, t2)) { // join tuples if they pass predicate
	    			joinTuple = new Tuple(getTupleDesc());

	    			int tuple1Length = t1.getTupleDesc().numFields();
	    			for (int i = 0; i < tuple1Length; i++)
	    				joinTuple.setField(i, t1.getField(i));
	    			
	    			int tuple2Length = t2.getTupleDesc().numFields();
	    			for (int i = 0; i < tuple2Length; i++)
	    				joinTuple.setField(i + tuple1Length, t2.getField(i));
	    		}
	    		
	    		this.increment(key1, key2);
    			
	    		if (joinTuple == null) // continue until two tuples join
	    			continue;
	    		else
	    			return joinTuple;
			}
			
			return null;
		}
		
		private void increment(Field key1, Field key2) {
			if (this._r2Index < this._r2.get(key2).size() - 1) { // check next tuple in relation 2's _key2Index bucket 
				this._r2Index++;
				return;
			} else
				this._r2Index = 0;
			
			
			switch (this._jp.getOperator()) {
				case EQUALS:
					if (this._r1Index < this._r1.get(key1).size() - 1) { // check next tuple in relation 1's _key1Index bucket
						this._r1Index++;
						return;
					} else
						this._r1Index = 0;
					
					if (this._key1Index < this._keys1.size() - 1) { // check next bucket for relation 1
						this._key2Index = ++this._key1Index; // increment key1Index first then set key2Index to that value
					} else {
						this._key1Index = this._keys1.size();	// all tuples have been checked
						this._key2Index = this._keys2.size();
					}
					return;
					
				case GREATER_THAN_OR_EQ: // same logic as GREATER_THAN
					
				case GREATER_THAN:
					if (this._key2Index < this._keys2.size() - 1 // check next bucket for relation 2
						&& this._keys1.get(this._key1Index).compare(this._jp.getOperator(), this._keys2.get(++this._key2Index)))
						return; // incremented key2Index above
					else
						this._key2Index = 0; // if key2 doesn't match none of the above keys will because keys are in sorted order
					
					break;
					
				case LESS_THAN_OR_EQ: // same logic as LESS_THAN
					
				case LESS_THAN:
					while (this._key2Index < this._keys2.size() - 1) {
						if (this._keys1.get(this._key1Index).compare(this._jp.getOperator(), this._keys2.get(++this._key2Index)))
							return; // return if it passes otherwise continue incrementing key2Index
					}
					
					this._key2Index = 0;
					
					break;
				
				case LIKE: // currently not logic implemented for LIKE and NOT_EQUALS
					
				case NOT_EQUALS:
					if (this._key2Index < this._keys2.size() - 1) { // check next bucket for relation 2
						this._key2Index++;
						return;
					} else
						this._key2Index = 0;
					
					break;
					
				default:
					System.out.println("Should never reach here.");
					
			}

			if (this._r1Index < this._r1.get(key1).size() - 1) { // check next tuple in relation 1's _key1Index bucket
				this._r1Index++;
				return;
			} else
				this._r1Index = 0;
			
			if (this._key1Index < this._keys1.size() - 1) { // check next bucket for relation 1
				this._key1Index++;
			} else {
				this._key1Index = this._keys1.size();	// all tuples have been checked
				this._key2Index = this._keys2.size();
			}
		}
	}

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this._p = p;
        this._child1 = child1;
        this._child2 = child2;
        this._hj = new HashJoin();
    }

    public JoinPredicate getJoinPredicate() {
        return this._p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
    	return this._child1.getTupleDesc().getFieldName(this._p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return this._child2.getTupleDesc().getFieldName(this._p.getField1());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
    	if (this._joinTupleDesc == null) {
    		this._joinTupleDesc = simpledb.TupleDesc.merge(this._child1.getTupleDesc(), this._child2.getTupleDesc());
    	}
    	
    	return this._joinTupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        this._child1.open();
        this._child2.open();
        this._hj.open(this._child1, this._child2, this._p);
    }

    public void close() {
        super.close();
        this._child1.close();
        this._child2.close();
        this._hj.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this._child1.rewind();
        this._child2.rewind();
        this._hj.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (this._hj.hasNext())
    		return this._hj.next();
    	
    	return null;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] { this._child1, this._child2 };
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	if (this._child1 != children[0]) {
    	    this._child1 = children[0];
    	}
    	if (this._child2 != children[0]) {
    	    this._child2 = children[0];
    	}
    }

}
