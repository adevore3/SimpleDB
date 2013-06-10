package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockPool
{
    private HashMap<PageId, Integer> _readLocks;
    private HashMap<PageId, Integer> _writeLocks;
    private HashMap<PageId, HashMap<TransactionId, Permissions>> _holding;
    private HashMap<TransactionId, HashMap<PageId, Permissions>> _waiting;
    private HashMap<PageId, Lock> _pool;
    private HashMap<PageId, Condition> _conditions;
    private HashSet<TransactionId> _deadLockCycle;
    public Lock _lock;

    public LockPool()
    {
        this._readLocks = new HashMap<PageId, Integer>();
        this._writeLocks = new HashMap<PageId, Integer>();
        this._holding = new HashMap<PageId, HashMap<TransactionId, Permissions>>();
        this._waiting = new HashMap<TransactionId, HashMap<PageId, Permissions>>();
        this._pool = new HashMap<PageId, Lock>();
        this._conditions = new HashMap<PageId, Condition>();
        this._deadLockCycle = null;
        this._lock = new ReentrantLock();
    }

    public void pageLockSetup(PageId pid)
    {
        if (!this._pool.containsKey(pid))
            this._pool.put(pid, new ReentrantLock());

        if (!this._conditions.containsKey(pid))
            this._conditions.put(pid, this._pool.get(pid).newCondition());

        if (!this._readLocks.containsKey(pid))
            this._readLocks.put(pid, 0);

        if (!this._writeLocks.containsKey(pid))
            this._writeLocks.put(pid, 0);
    }

    public Lock getPageLock(PageId pid)
    {
        return this._pool.get(pid);
    }

    public Permissions holdsLock(TransactionId tid, PageId pid)
    {
        if (this._holding.containsKey(pid))
            return this._holding.get(pid).get(tid);

        return null;
    }

    public boolean isLocked(PageId pid)
    {
        if (this._holding.containsKey(pid))
            return this._holding.get(pid).size() > 0;

        return false;
    }

    public ArrayList<PageId> getHeldPages(TransactionId tid)
    {
        ArrayList<PageId> result = new ArrayList<PageId>();

        for (PageId pid : this._holding.keySet())
            if (this._holding.get(pid).containsKey(tid))
                result.add(pid);

        return result;
    }

    public void removePage(PageId pid)
    {
        this._readLocks.remove(pid);
        this._writeLocks.remove(pid);
        this._holding.remove(pid);

        for (TransactionId tid : this._waiting.keySet())
            this._waiting.get(tid).remove(pid);

        this._pool.remove(pid);
        this._conditions.remove(pid);
    }

    private void addHolding(PageId pid, TransactionId tid, Permissions perm)
    {
        if (this._holding.containsKey(pid))
        {
            this._holding.get(pid).put(tid, perm);
        }
        else
        {
            HashMap<TransactionId, Permissions> hm = new HashMap<TransactionId, Permissions>();
            hm.put(tid, perm);
            this._holding.put(pid, hm);
        }
    }

    private boolean removeHolding(PageId pid, TransactionId tid,
            Permissions perm)
    {
        if (this._holding.containsKey(pid))
            if (this._holding.get(pid).containsKey(tid))
                if (this._holding.get(pid).get(tid).equals(perm))
                {
                    this._holding.get(pid).remove(tid);
                    return true;
                }

        return false;
    }

    private void addWaiting(TransactionId tid, PageId pid, Permissions perm)
    {
        if (this._waiting.containsKey(tid))
        {
            this._waiting.get(tid).put(pid, perm);
        }
        else
        {
            HashMap<PageId, Permissions> hs = new HashMap<PageId, Permissions>();
            hs.put(pid, perm);
            this._waiting.put(tid, hs);
        }
    }

    private void removeWaiting(TransactionId tid, PageId pid)
    {
        if (this._waiting.containsKey(tid))
            this._waiting.remove(pid);
    }

    public void acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException
    {
        if (tid == null)
            return;

        this.pageLockSetup(pid);
        
        this._pool.get(pid).lock();
        try
        {
            Permissions oldPerm; // old permissions
            if (!this._holding.containsKey(pid))
                oldPerm = null;
            else
                oldPerm = this._holding.get(pid).get(tid);

            if (oldPerm == null)
            { // tid didn't have a lock before
                if (perm.equals(Permissions.READ_ONLY))
                    this.acquireLock(tid, pid, perm, false);
                else
                    this.acquireLock(tid, pid, perm, false);
            }
            else
            { // tid did have a lock before
                if (oldPerm.equals(perm))
                { // tid trying to acquire the same lock it had before
                    return;
                }
                else
                { // tid is trying to acquire a different lock
                    if (oldPerm.equals(Permissions.READ_ONLY))
                    { // upgrade to Permissions.READ_WRITE
                        this.acquireLock(tid, pid, perm, true);
                    }
                    else
                    { // no need to downgrade
                        return;
                    }
                }
            }
        }
        finally
        {
            try
            {
                this._pool.get(pid).unlock();
            }
            catch (IllegalMonitorStateException e)
            {

            }
        }
    }

    public void releaseLocks(TransactionId tid)
    {
        for (PageId pid : this._holding.keySet())
            this.releaseLock(tid, pid);

        this._waiting.remove(tid);
    }

    public void releaseLock(TransactionId tid, PageId pid)
    {
        if (tid == null)
            return;

        if (!this._holding.get(pid).containsKey(tid))
            return;

        this._pool.get(pid).lock();
        try
        {
            Permissions perm = this._holding.get(pid).get(tid);

            if (perm.equals(Permissions.READ_ONLY))
                this.releaseReadLock(tid, pid);
            else
                this.releaseWriteLock(tid, pid);

            this._holding.get(pid).remove(tid);
            this._waiting.remove(tid);
        }
        catch (IllegalMonitorStateException e)
        {

        }
        finally
        {
            this._pool.get(pid).unlock();
        }
    }

    private void acquireLock(TransactionId tid, PageId pid, Permissions perm,
            boolean upgrade) throws TransactionAbortedException
    {
        while (this.lockTest(perm, pid, upgrade))
        {
            this.addWaiting(tid, pid, perm); // update waiting map

            HashSet<TransactionId> deadLockCycle = this
                    .detectDeadLock(tid, pid);
            if (deadLockCycle != null)
            {
                // if (this.resolveDeadLock(tid, pid, deadLockCycle))
                // {
                //
                // }

                throw new TransactionAbortedException(Thread.currentThread()
                        .getName()
                        + " -> Deadlock detected with transaction "
                        + tid.getId() + " and page " + pid.pageNumber() + ".");

            }

            this._conditions.get(pid).awaitUninterruptibly();
        }

        this.lockUpdate(perm, pid, upgrade);

        // update status of waiting and holding
        this.removeWaiting(tid, pid);
        this.addHolding(pid, tid, perm);
    }

    private boolean lockTest(Permissions perm, PageId pid, boolean upgrade)
    {
        boolean result = this._writeLocks.get(pid).intValue() != 0;
        if (perm.equals(Permissions.READ_WRITE))
        {
            int allowedReadLocks = (upgrade) ? 1 : 0;
            result = result
                    || this._readLocks.get(pid).intValue() != allowedReadLocks;
        }

        return result;
    }

    private void lockUpdate(Permissions perm, PageId pid, boolean upgrade)
    {
        if (perm.equals(Permissions.READ_ONLY))
        {
            this._readLocks.put(pid, this._readLocks.get(pid).intValue() + 1);
        }
        else
        {
            this._writeLocks.put(pid, this._writeLocks.get(pid).intValue() + 1);
            if (upgrade)
                this._readLocks.put(pid, 0);
        }
    }

    private void releaseReadLock(TransactionId tid, PageId pid)
    {
        if (this.removeHolding(pid, tid, Permissions.READ_ONLY))
        {
            this._readLocks.put(pid, this._readLocks.get(pid).intValue() - 1);
            this._conditions.get(pid).signalAll();
        }
    }

    private void releaseWriteLock(TransactionId tid, PageId pid)
    {
        if (this.removeHolding(pid, tid, Permissions.READ_WRITE))
        {
            this._writeLocks.put(pid, 0);
            this._conditions.get(pid).signalAll();
        }
    }

    private synchronized HashSet<TransactionId> detectDeadLock(
            TransactionId tid, PageId pid)
    {
        return detectDeadLock(tid, pid, new HashSet<TransactionId>());
    }

    private synchronized HashSet<TransactionId> detectDeadLock(
            TransactionId tid, PageId pid, HashSet<TransactionId> visited)
    {
        if (!this._holding.containsKey(pid))
            return null;

        if (visited.contains(tid))
            return visited;

        visited.add(tid);

        for (TransactionId tkey : this._holding.get(pid).keySet())
            if (!tkey.equals(tid))
            {
                if (this._waiting.containsKey(tkey))
                    for (PageId pkey : this._waiting.get(tkey).keySet())
                    {
                        HashSet<TransactionId> result = detectDeadLock(tkey,
                                pkey, visited);
                        if (result != null)
                            return result;
                    }

                visited.remove(tkey);
            }

        return null;
    }

    /**
     * Resolves deadlocks by telling which threads to abort.
     * 
     * @param tid
     * @param pid
     * @param cycle
     * @return True if that thread should abort and false otherwise.
     */
    private synchronized boolean resolveDeadLock(TransactionId tid, PageId pid,
            HashSet<TransactionId> cycle)
    {
        if (this._deadLockCycle == null)
            this._deadLockCycle = cycle;

        // System.out.println("New deadlock cycle:");
        // System.out.println(this.deadLockCycleToString());

        this._deadLockCycle.remove(tid); // remove tid from set
        if (this._deadLockCycle.size() == 0) // should not abort if last tid to be removed
        {
            this._deadLockCycle = null;
            return false;
        }
        else
        {
            return true;
        }
    }

    public synchronized String toStringLockPool()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(this.readLocksToString());
        sb.append("\n");
        sb.append(this.writeLocksToString());
        sb.append("\n");
        sb.append(this.holdingToString());
        sb.append("\n");
        sb.append(this.waitingToString());
        sb.append("\n");
        sb.append(this.deadLockCycleToString(this._deadLockCycle));
        sb.append("\n");
        return sb.toString();
    }

    public String readLocksToString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("---Read Locks\n");
        for (PageId pid : this._readLocks.keySet())
            sb.append("pid: " + pid.pageNumber() + " -> "
                    + this._readLocks.get(pid) + "\n");
        return sb.toString();
    }

    public String writeLocksToString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("---Write Locks\n");
        for (PageId pid : this._writeLocks.keySet())
            sb.append("pid: " + pid.pageNumber() + " -> "
                    + this._writeLocks.get(pid) + "\n");
        return sb.toString();
    }

    public String holdingToString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("---Holding\n");
        for (PageId pid : this._holding.keySet())
        {
            sb.append("pid: " + pid.pageNumber() + "\n");
            for (TransactionId tid : this._holding.get(pid).keySet())
                sb.append("\ttid: " + tid.getId() + " perm: "
                        + this._holding.get(pid).get(tid) + "\n");
        }
        return sb.toString();
    }

    public String waitingToString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("---Waiting\n");
        for (TransactionId tid : this._waiting.keySet())
        {
            sb.append("tid: " + tid.getId() + "\n");
            for (PageId pid : this._waiting.get(tid).keySet())
                sb.append("\tpid: " + pid.pageNumber() + " perm: "
                        + this._waiting.get(tid).get(pid) + "\n");
        }
        return sb.toString();
    }

    public String deadLockCycleToString(HashSet<TransactionId> deadLockCycle)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("---Deadlock Cycle\n");
        if (deadLockCycle != null)
            for (TransactionId tid : deadLockCycle)
                sb.append("\ttid: " + tid.getId() + "\n");

        return sb.toString();
    }
}
