package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes them from the table
 * they belong to.
 */
public class Delete extends Operator
{

    private static final long serialVersionUID = 1L;

    private TransactionId _tid;
    private DbIterator _child;
    private final TupleDesc _td;
    private boolean _complete;

    /**
     * Constructor specifying the transaction that this delete belongs to as well as the child to
     * read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child)
    {
        this._tid = t;
        this._child = child;
        this._td = new TupleDesc(new Type[]
        { Type.INT_TYPE }, new String[]
        { "null" });
        this._complete = false;
    }

    public TupleDesc getTupleDesc()
    {
        return this._child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException
    {
        super.open();
        this._child.open();
    }

    public void close()
    {
        super.close();
        this._child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException
    {
        this._child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are processed via the buffer
     * pool (which can be accessed via the Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException
    {
        if (this._complete)
        { // delete has already been performed
            return null;
        }
        else
        {
            this._complete = true;
        }

        int count = 0;
        while (this._child.hasNext())
        {
            Tuple t = this._child.next();
            // delete tuple and update number of tuples deleted
            Database.getBufferPool().deleteTuple(this._tid, t);
            count++;
        }

        Tuple result = new Tuple(this._td);
        result.setField(0, new IntField(count)); // tuple holding number of deletes

        return result;
    }

    @Override
    public DbIterator[] getChildren()
    {
        return new DbIterator[]
        { this._child };
    }

    @Override
    public void setChildren(DbIterator[] children)
    {
        if (this._child != children[0])
            this._child = children[0];
    }

}
