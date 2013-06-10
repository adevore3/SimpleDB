package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk. Access methods call
 * into it to retrieve pages, and it fetches pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a page, BufferPool
 * checks that the transaction has the appropriate locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool
{
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /**
     * Default number of pages passed to the constructor. This is used by other classes. BufferPool
     * should use the numPages argument to the constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int _numPages;
    private ConcurrentHashMap<PageId, Page> _cache;
    private LockPool _lockPool;
    private List<PageId> _evictionQueue;
    private ConcurrentHashMap<PageId, PageId> _pids;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     * 
     * @param numPages
     *            maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages)
    {
        this._numPages = numPages;
        this._cache = new ConcurrentHashMap<PageId, Page>();
        this._lockPool = new LockPool();
        this._evictionQueue = new ArrayList<PageId>();
        this._pids = new ConcurrentHashMap<PageId, PageId>();
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire a lock and may
     * block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is present, it should be
     * returned. If it is not present, it should be added to the buffer pool and returned. If there
     * is insufficient space in the buffer pool, an page should be evicted and the new page should
     * be added in its place.
     * 
     * @param tid
     *            the ID of the transaction requesting the page
     * @param pid
     *            the ID of the requested page
     * @param perm
     *            the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException
    {
        PageId true_pid; // reference to page's actual pid
        synchronized (this._lockPool._lock)
        {
            while (this._cache.size() >= this._numPages)
                this.evictPage(); // evict pages till we are within limit

            if (!this._pids.containsKey(pid))
            {
                DbFile dbf = Database.getCatalog().getDbFile(pid.getTableId());
                Page p = dbf.readPage(pid);

                true_pid = p.getId();

                this._pids.put(pid, true_pid); // store true pid
                this._cache.put(true_pid, p); // store page
                this._lockPool.pageLockSetup(true_pid); // initialize page locks
                this._evictionQueue.add(true_pid); // store true pid in eviction queue
            }
            else
            {
                true_pid = this._pids.get(pid);

                for (int i = 0; i < this._evictionQueue.size(); i++)
                { // update eviction queue
                    if (this._evictionQueue.get(i).equals(true_pid))
                    {
                        this._evictionQueue.remove(i);
                    }
                }
                this._evictionQueue.add(true_pid); // add pid to back of eviction queue
            }
        }

        this._lockPool.acquireLock(tid, true_pid, perm);

        return this._cache.get(true_pid);
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in wrong behavior.
     * Think hard about who needs to call this and why, and why they can run the risk of calling it.
     * 
     * @param tid
     *            the ID of the transaction requesting the unlock
     * @param pid
     *            the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid)
    {
        this._lockPool.releaseLock(tid, this._pids.get(pid));
    }

    /**
     * Release all locks associated with a given transaction.
     * 
     * @param tid
     *            the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException
    {
        this.transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId pid)
    {
        return this._lockPool.holdsLock(tid, this._pids.get(pid)) != null;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the transaction.
     * 
     * @param tid
     *            the ID of the transaction requesting the unlock
     * @param commit
     *            a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException
    {
        if (commit)
        { // successful commit
            for (PageId pid : this._lockPool.getHeldPages(tid))
            {
                // this.flushPages(tid); // flushes all pages dirtied by this transaction
                this.flushPage(pid);
                this._cache.get(pid).setBeforeImage();
            }
        }
        else
        { // abort
            for (PageId pid : this._lockPool.getHeldPages(tid))
            {
                DbFile dbf = Database.getCatalog().getDbFile(pid.getTableId());
                Page p = dbf.readPage(pid);
                this._cache.put(pid, p);
            }
        }

        this._lockPool.releaseLocks(tid); // release page from locks held by this tid

        if (!commit)
            try
            {
                Thread.sleep((long) Math.random() * 100);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid. Will acquire a write lock on
     * the page the tuple is added to(Lock acquisition is not needed for lab2). May block if the
     * lock cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit,
     * and updates cached versions of any pages that have been dirtied so that future requests see
     * up-to-date pages.
     * 
     * @param tid
     *            the transaction adding the tuple
     * @param tableId
     *            the table to add the tuple to
     * @param t
     *            the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException
    {
        HeapFile hf = (HeapFile) Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> dirtyPages = hf.insertTuple(tid, t);

        for (Page p : dirtyPages)
        {
            p.markDirty(true, tid); // mark page as dirty
            this._cache.put(p.getId(), p); // store dirty page
        }
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on the page the
     * tuple is removed from. May block if the lock cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit.
     * Does not need to update cached versions of any pages that have been dirtied, as it is not
     * possible that a new page was created during the deletion (note difference from addTuple).
     * 
     * @param tid
     *            the transaction deleting the tuple.
     * @param t
     *            the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException
    {
        HeapFile hf = (HeapFile) Database.getCatalog().getDbFile(
                t.getRecordId().getPageId().getTableId());
        HeapPage hp = (HeapPage) hf.deleteTuple(tid, t);
        hp.markDirty(true, tid); // mark page as dirty
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes dirty data to
     * disk so will break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException
    {
        for (PageId pid : this._evictionQueue)
            this.flushPage(pid);
    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery manager to ensure
     * that the buffer pool doesn't keep a rolled back page in its cache.
     */
    public synchronized void discardPage(PageId pid)
    {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid
     *            an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException
    {
        if (this._cache.containsKey(pid))
        {
            Page p = this._cache.get(pid);

            TransactionId tid = p.isDirty();
            if (tid != null)
            {
                // write and force log
                LogFile lf = Database.getLogFile();
                lf.logWrite(tid, p.getBeforeImage(), p);
                lf.force();

                // write page
                p.markDirty(false, tid);
                DbFile dbf = Database.getCatalog().getDbFile(pid.getTableId());
                dbf.writePage(p);
            }
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException
    {
        for (PageId pid : this._evictionQueue)
        {
            Page p = this._cache.get(pid);
            if (p.isDirty() == tid) // flush only if dirty
            {
                this.flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure dirty pages are
     * updated on disk.
     */
    private synchronized void evictPage() throws DbException
    {
        try
        {
            boolean flushPage = false;
            for (PageId pid : this._evictionQueue)
            {
                if (this._cache.get(pid).isDirty() == null)
                { // if page is not dirtied
                    this.flushPage(pid);

                    this._cache.remove(pid); // remove pid from cache
                    this._lockPool.removePage(pid); // remove pid from lock pool
                    this._evictionQueue.remove(pid); // remove pid from eviction queue
                    this._pids.remove(pid);

                    flushPage = true;
                    break;
                }
            }

            if (!flushPage)
                throw new DbException("All pages are dirty.");
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new DbException("Problem flushing page.");
        }
    }
}
