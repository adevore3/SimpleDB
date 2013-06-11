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
        for (SocketInfo w : workers)
        {
            this._workerIdToIndex.put(w.getId(), i++);
        }
        this._workerEOS = new BitSet(workers.length);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException
    {
        this._tuples = null;
        this._innerBuffer = new ArrayList<TupleBag>();
        this._innerBufferIndex = 0;
        if (this._child != null)
            this._child.open();
        super.open();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException
    {
        this._tuples = null;
        this._innerBufferIndex = 0;
    }

    @Override
    public void close()
    {
        super.close();
        this.setBuffer(null);
        this._tuples = null;
        this._innerBufferIndex = -1;
        this._innerBuffer = null;
        this._workerEOS.clear();
    }

    @Override
    public TupleDesc getTupleDesc()
    {
        if (this._child != null)
            return this._child.getTupleDesc();

        return null;
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
        TupleBag tb = null;
        if (this._innerBufferIndex < this._innerBuffer.size())
            return this._innerBuffer.get(this._innerBufferIndex++).iterator();

        while (this._workerEOS.nextClearBit(0) < this._workers.length)
        {
            tb = (TupleBag) this.take(-1);
            if (tb.isEos()) {
                this._workerEOS.set(this._workerIdToIndex.get(tb.getWorkerID()));
            } else {
                this._innerBuffer.add(tb);
                this._innerBufferIndex++;
                return tb.iterator();
            }
        }
        
        return null;
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException
    {
        while (this._tuples == null || ! this._tuples.hasNext()) {
            try {
                this._tuples = getTuples();
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new DbException(e.getLocalizedMessage());
            }
            
            if (this._tuples == null)
                return null;
        }
        
        return this._tuples.next();
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
        this._child = (CollectProducer) children[0];
    }
}
