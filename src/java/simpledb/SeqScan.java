package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId _tid;
    private int _tableid;
    private String _tableAlias;
    private DbFileIterator _tupleIt;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	this._tid = tid;
    	this._tableid = tableid;
    	this._tableAlias = tableAlias;
    	this._tupleIt = Database.getCatalog().getDbFile(this._tableid).iterator(this._tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this._tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        return this._tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this._tableid = tableid;
        this._tableAlias = tableAlias;
        this._tupleIt = Database.getCatalog().getDbFile(this._tableid).iterator(this._tid);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
    	this._tupleIt.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	TupleDesc td = Database.getCatalog().getTupleDesc(this._tableid);
    	
    	Type[] typeAr = new Type[td.numFields()];
        String[] fieldAr = new String[td.numFields()];
    	
        for (int i = 0; i < td.numFields(); i++) {
        	typeAr[i] = td.getFieldType(i);
        	
    		if (this._tableAlias == null)
    			fieldAr[i] = "null." + td.getFieldName(i);
    		else
    			fieldAr[i] = this._tableAlias + "." + td.getFieldName(i);
    	}
    	
        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return this._tupleIt.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
		return this._tupleIt.next();
    }

    public void close() {
        this._tupleIt.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this._tupleIt.rewind();
    }
}
