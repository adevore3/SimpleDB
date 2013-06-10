package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    
    private Predicate _p;
    private DbIterator _child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this._p = p;
        this._child = child;
    }

    public Predicate getPredicate() {
        return this._p;
    }

    public TupleDesc getTupleDesc() {
        return this._child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open();
        this._child.open();
    }

    public void close() {
    	super.close();
        this._child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this._child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	while (this._child.hasNext()) {
            Tuple t = this._child.next();
            
            if (this._p.filter(t))
            	return t;
        }
    	
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] { this._child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	if (this._child != children[0]) {
    	    this._child = children[0];
    	}
    }

}
