package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the constructor
 */
public class Insert extends Operator
{

    private static final long serialVersionUID = 1L;

    private TransactionId _tid;
    private DbIterator _child;
    private int _tableid;
    private final TupleDesc _td;
    private boolean _complete;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException
    {
        this._tid = t;
        this._child = child;
        this._tableid = tableid;
        this._td = new TupleDesc(new Type[]
        { Type.INT_TYPE }, new String[]
        { "null" });
        this._complete = false;
    }

    public TupleDesc getTupleDesc()
    {
        return this._td;
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
     * Inserts tuples read from child into the tableid specified by the constructor. It returns a
     * one field tuple containing the number of inserted records. Inserts should be passed through
     * BufferPool. An instances of BufferPool is available via Database.getBufferPool(). Note that
     * insert DOES NOT need check to see if a particular tuple is a duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or null if called more
     *         than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException
    {
        if (this._complete)
        { // insert has already been performed
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
            // insert tuple and update number of tuples inserted
            try
            {
                Database.getBufferPool().insertTuple(this._tid, this._tableid, t);
            }
            catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            count++;
        }

        Tuple result = new Tuple(this._td);
        result.setField(0, new IntField(count)); // tuple holding number of inserts

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
        {
            this._child = children[0];
        }
    }
}
