package simpledb.parallel;

import java.util.ArrayList;

import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.session.IoSession;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some partition function (provided
 * as a PartitionFunction object during the ShuffleProducer's instantiation).
 * 
 * */
public class ShuffleProducer extends Producer
{

    private static final long serialVersionUID = 1L;

    private transient WorkingThread _runningThread;

    private DbIterator _child;
    private SocketInfo[] _workers;
    private PartitionFunction<?, ?> _pf;

    public String getName()
    {
        return "shuffle_p";
    }

    public ShuffleProducer(DbIterator child, ParallelOperatorID operatorID,
            SocketInfo[] workers, PartitionFunction<?, ?> pf)
    {
        super(operatorID);
        this._child = child;
        this._workers = workers;
        this._pf = pf;
    }

    public void setPartitionFunction(PartitionFunction<?, ?> pf)
    {
        this._pf = pf;
    }

    public SocketInfo[] getWorkers()
    {
        return this._workers;
    }

    public PartitionFunction<?, ?> getPartitionFunction()
    {
        return this._pf;
    }

    class WorkingThread extends Thread
    {
        public void run()
        {
            int numPartition = ShuffleProducer.this._pf.numPartition;
            IoSession sessions[] = new IoSession[numPartition];
            ArrayList<ArrayList<Tuple>> buffers = new ArrayList<ArrayList<Tuple>>();
            long[] lastTimes = new long[numPartition];
            TupleDesc td = ShuffleProducer.this.getTupleDesc();

            // initialize sessions and buffers
            for (int i = 0; i < numPartition; i++)
            {
                SocketInfo w = ShuffleProducer.this._workers[i];
                sessions[i] = ParallelUtility.createSession(w.getAddress(),
                        ShuffleProducer.this.getThisWorker().minaHandler, -1);
                buffers.add(new ArrayList<Tuple>());
            }

            try
            {
                while (ShuffleProducer.this._child.hasNext())
                {
                    Tuple tup = ShuffleProducer.this._child.next();
                    int partition = ShuffleProducer.this._pf.partition(tup, td);

                    buffers.get(partition).add(tup);
                    lastTimes[partition] = System.currentTimeMillis();

                    int cnt = buffers.get(partition).size();
                    if (cnt >= TupleBag.MAX_SIZE)
                    {
                        sessions[partition].write(new TupleBag(
                                ShuffleProducer.this.operatorID,
                                ShuffleProducer.this.getThisWorker().workerID,
                                buffers.get(partition).toArray(new Tuple[]
                                {}), td));
                        buffers.get(partition).clear();
                        lastTimes[partition] = System.currentTimeMillis();
                    }
                    if (cnt >= TupleBag.MIN_SIZE)
                    {
                        long thisTime = System.currentTimeMillis();
                        if (thisTime - lastTimes[partition] > TupleBag.MAX_MS)
                        {
                            sessions[partition]
                                    .write(new TupleBag(
                                            ShuffleProducer.this.operatorID,
                                            ShuffleProducer.this
                                                    .getThisWorker().workerID,
                                            buffers.get(partition).toArray(
                                                    new Tuple[]
                                                    {}), td));
                            buffers.get(partition).clear();
                            lastTimes[partition] = thisTime;
                        }
                    }
                }

                // any tuples leftover need to be sent
                for (int i = 0; i < numPartition; i++)
                {
                    if (buffers.get(i).size() > 0)
                        sessions[i].write(new TupleBag(
                                ShuffleProducer.this.operatorID,
                                ShuffleProducer.this.getThisWorker().workerID,
                                buffers.get(i).toArray(new Tuple[]
                                {}), td));

                    sessions[i]
                            .write(new TupleBag(
                                    ShuffleProducer.this.operatorID,
                                    ShuffleProducer.this.getThisWorker().workerID))
                            .addListener(new IoFutureListener<WriteFuture>()
                            {

                                @Override
                                public void operationComplete(WriteFuture future)
                                {
                                    ParallelUtility.closeSession(future
                                            .getSession());
                                }
                            });// .awaitUninterruptibly(); //wait until all the data have
                               // successfully
                               // transfered
                }
            }
            catch (DbException e)
            {
                e.printStackTrace();
            }
            catch (TransactionAbortedException e)
            {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException
    {
        this._child.open();
        this._runningThread = new WorkingThread();
        this._runningThread.start();
        super.open();
    }

    public void close()
    {
        super.close();
        this._child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleDesc getTupleDesc()
    {
        return this._child.getTupleDesc();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException
    {
        try
        {
            this._runningThread.join();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        return null;
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
        this._child = children[0];
    }
}
