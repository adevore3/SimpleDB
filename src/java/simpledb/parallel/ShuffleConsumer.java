package simpledb.parallel;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.parallel.Exchange.ParallelOperatorID;

/**
 * The consumer part of the Shuffle Exchange operator.
 * 
 * A ShuffleProducer operator sends tuples to all the workers according to some PartitionFunction,
 * while the ShuffleConsumer (this class) encapsulates the methods to collect the tuples received at
 * the worker from multiple source workers' ShuffleProducer.
 * 
 * */
public class ShuffleConsumer extends Consumer
{

    private static final long serialVersionUID = 1L;
    
    private transient Iterator<Tuple> _tuples;
    
    private transient int _innerBufferIndex;
    private transient ArrayList<TupleBag> _innerBuffer;

    private DbIterator _child;
    private ParallelOperatorID _operatorID;
    private final BitSet _workerEOS;
    private final SocketInfo[] _workers;
    private final HashMap<String, Integer> _workerIdToIndex;

    public String getName()
    {
        return "shuffle_c";
    }

    public ShuffleConsumer(ParallelOperatorID operatorID, SocketInfo[] workers)
    {
        this(null, operatorID, workers);
    }

    public ShuffleConsumer(ShuffleProducer child,
            ParallelOperatorID operatorID, SocketInfo[] workers)
    {
        super(operatorID);
        this._child = child;
        this._operatorID = operatorID;
        this._workers = workers;
        this._workerIdToIndex = new HashMap<String, Integer>();
        int i = 0;
        for (SocketInfo w : workers) {
        	this._workerIdToIndex.put(w.getId(), i++);
        }
        this._workerEOS = new BitSet(workers.length);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException
    {
        this._child.open();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException
    {
        this._child.rewind();
    }

    @Override
    public void close()
    {
        this._child.close();
    }

    @Override
    public TupleDesc getTupleDesc()
    {
        return this._child.getTupleDesc();
    }

    /**
     * 
     * Retrieve a batch of tuples from the buffer of ExchangeMessages. Wait if the buffer is empty.
     * 
     * @return Iterator over the new tuples received from the source workers. Return
     *         <code>null</code> if all source workers have sent an end of file message.
     */
    Iterator<Tuple> getTuples() throws InterruptedException
    {
        // some code goes here
        return null;
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException
    {
        return this._child.next();
    }

    @Override
    public DbIterator[] getChildren()
    {
        return new DbIterator[] { this._child };
    }

    @Override
    public void setChildren(DbIterator[] children)
    {
        this._child = children[0];
    }
}
