package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). Note that we only
 * support aggregates over a single column, grouped by a single column.
 */
public class Aggregate extends Operator
{

    private static final long serialVersionUID = 1L;

    private DbIterator _child;
    private int _afield;
    private int _gfield;
    private Aggregator.Op _aop;
    private boolean _open;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to construct an
     * {@link IntAggregator} or {@link StringAggregator} to help you with your implementation of
     * readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop)
    {
        this._afield = afield;
        this._gfield = gfield;
        this._aop = aop;
        this._child = child;
        this._open = false;
    }

    private void performAggregate()
    {
        TupleDesc td = this._child.getTupleDesc();
        Type gfieldtype = (this._gfield == Aggregator.NO_GROUPING) ? null : td
                .getFieldType(this._gfield);
        Aggregator a;

        switch (td.getFieldType(this._afield))
        {
        case INT_TYPE:
            a = new IntegerAggregator(this._gfield, gfieldtype, this._afield,
                    this._aop);
            break;
        case STRING_TYPE:
            a = new StringAggregator(this._gfield, gfieldtype, this._afield,
                    this._aop);
            break;
        default:
            throw new IllegalArgumentException();
        }

        try
        {
            this._child.open();
            while (this._child.hasNext())
            {
                a.mergeTupleIntoGroup(this._child.next());
            }

            this._child.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }

        this._child = a.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby field index in the
     *         <b>INPUT</b> tuples. If not, return {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField()
    {
        return this._gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name of the groupby field
     *         in the <b>OUTPUT</b> tuples If not, return null;
     * */
    public String groupFieldName()
    {
        return this._child.getTupleDesc().getFieldName(this._gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField()
    {
        return this._afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     * */
    public String aggregateFieldName()
    {
        return this._child.getTupleDesc().getFieldName(this._afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp()
    {
        return this._aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop)
    {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException
    {
        super.open();
        this._child.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first field is the field by
     * which we are grouping, and the second field is the result of computing the aggregate, If
     * there is no group by field, then the result tuple should contain one field representing the
     * result of the aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException
    {
        if (!this._open)
        {
            this.performAggregate();
            this._child.open();
            this._open = true;
        }

        if (this._child.hasNext())
            return this._child.next();
        else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException
    {
        this._child.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field, this will have one
     * field - the aggregate column. If there is a group by field, the first field will be the group
     * by field, and the second will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are given in the
     * constructor, and child_td is the TupleDesc of the child iterator.
     */
    public TupleDesc getTupleDesc()
    {
        return this._child.getTupleDesc();
    }

    public void close()
    {
        super.close();
        this._child.close();
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
