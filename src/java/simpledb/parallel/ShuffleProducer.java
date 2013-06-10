package simpledb.parallel;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some
 * partition function (provided as a PartitionFunction object during the
 * ShuffleProducer's instantiation).
 * 
 * */
public class ShuffleProducer extends Producer {

    private static final long serialVersionUID = 1L;
    
    private DbIterator _child;
    private ParallelOperatorID _operatorID;
    private SocketInfo[] _workers;
    private PartitionFunction<?, ?> _pf;

    public String getName() {
        return "shuffle_p";
    }

    public ShuffleProducer(DbIterator child, ParallelOperatorID operatorID,
            SocketInfo[] workers, PartitionFunction<?, ?> pf) {
        super(operatorID);
        this._child = child;
        this._operatorID = operatorID;
        this._workers = workers;
        this._pf = pf;
    }

    public void setPartitionFunction(PartitionFunction<?, ?> pf) {
        this._pf = pf;
    }

    public SocketInfo[] getWorkers() {
        return this._workers;
    }

    public PartitionFunction<?, ?> getPartitionFunction() {
        return this._pf;
    }

    // some code goes here
    class WorkingThread extends Thread {
        public void run() {

            // some code goes here
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this._child.open();
    }

    public void close() {
        this._child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this._child.getTupleDesc();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        return this._child.next();
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this._child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this._child = children[0];
    }
}
