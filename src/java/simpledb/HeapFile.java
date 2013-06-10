package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular
 * order. Tuples are stored on pages, each of which is a fixed size, and the file is simply a
 * collection of those pages. HeapFile works closely with HeapPage. The format of HeapPages is
 * described in the HeapPage constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile
{

    private File _f;
    private TupleDesc _td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td)
    {
        this._f = f;
        this._td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile()
    {
        return this._f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note: you will need to
     * generate this tableid somewhere ensure that each HeapFile has a "unique id," and that you
     * always return the same value for a particular HeapFile. We suggest hashing the absolute file
     * name of the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId()
    {
        return this._f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc()
    {
        return this._td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid)
    {
        try
        {
            RandomAccessFile raf = new RandomAccessFile(this._f, "r"); // allows file access

            byte[] b = new byte[BufferPool.PAGE_SIZE];

            raf.seek(pid.pageNumber() * BufferPool.PAGE_SIZE); // move reading pointer in file
            raf.read(b, 0, BufferPool.PAGE_SIZE);

            raf.close();

            return new HeapPage((HeapPageId) pid, b);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return null; // return nothing if reading a page failed
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException
    {
        try
        {
            RandomAccessFile raf = new RandomAccessFile(this._f, "rw"); // allows file access
            byte[] b = page.getPageData(); // data to write

            raf.seek(page.getId().pageNumber() * BufferPool.PAGE_SIZE); // move writing pointer in
            raf.write(b, 0, BufferPool.PAGE_SIZE);
//            raf.write(b);
            raf.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages()
    {
        return (int) Math.ceil(this._f.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException
    {
        ArrayList<Page> result = new ArrayList<Page>();

        BufferPool bp = Database.getBufferPool();

        for (int i = 0; i < this.numPages(); i++)
        {
            HeapPageId pid = new HeapPageId(this.getId(), i); // get page id
            HeapPage hp = (HeapPage) bp
                    .getPage(tid, pid, Permissions.READ_ONLY);

            if (hp.getNumEmptySlots() > 0)
            {
                hp = (HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);
                hp.insertTuple(t);
                result.add(hp);
                break;
            }
        }

        if (result.size() > 0)
        {
            return result;
        }
        else
        {
            synchronized (this._f)
            {
                HeapPageId pid = new HeapPageId(this.getId(), this.numPages());
                HeapPage hp = new HeapPage(pid, HeapPage.createEmptyPageData());
                hp.insertTuple(t);
                result.add(hp);
                FileOutputStream fos = new FileOutputStream(this._f, true);
                fos.write(hp.getPageData()); // write new page to file
                fos.close();
            }

            return result;
        }
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException
    {
        BufferPool bp = Database.getBufferPool();
        HeapPage hp = (HeapPage) bp.getPage(tid, t.getRecordId().getPageId(),
                Permissions.READ_WRITE); // get page
        hp.deleteTuple(t);
        return hp;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid)
    {
        return new DbFileIter(tid);
    }

    public class DbFileIter implements DbFileIterator
    {
        private TransactionId _tid;
        private final int _numPages;
        private int _nextPage;
        private boolean _open;
        private Tuple _next;
        private Iterator<Tuple> _tupleIterator;

        public DbFileIter(TransactionId tid)
        {
            this._tid = tid;
            this._numPages = numPages();
            this._nextPage = 0;
            this._open = false;
            this._next = null;
            this._tupleIterator = null;
        }

        @Override
        public boolean hasNext() throws DbException,
                TransactionAbortedException
        {
            if (this._open && this._next == null)
                this._next = this.fetchNext();

            return this._next != null;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException
        {
            if (this._next == null)
            {
                this._next = fetchNext();
                if (this._next == null)
                    throw new NoSuchElementException();
            }

            Tuple result = this._next;
            this._next = null;
            return result;
        }

        public Tuple fetchNext() throws DbException,
                TransactionAbortedException, NoSuchElementException
        {
            if (!this._open)
                throw new NoSuchElementException("DbFileIterator not yet open.");

            if (this._next == null)
            {
                while (this._next == null)
                {
                    while (this._nextPage < this._numPages)
                    {
                        if (this._tupleIterator == null)
                        {
                            HeapPageId hpid = new HeapPageId(getId(),
                                    this._nextPage);
                            HeapPage hp = (HeapPage) Database.getBufferPool()
                                    .getPage(this._tid, hpid,
                                            Permissions.READ_ONLY);

                            this._tupleIterator = hp.iterator(); // save new iterator
                            this._nextPage++; // update to next page

                            // if iterator is empty then grab next page
                            if (!this._tupleIterator.hasNext())
                                this._tupleIterator = null;
                        }
                        else
                        {
                            break;
                        }
                    }

                    if (this._tupleIterator != null)
                        if (this._tupleIterator.hasNext())
                        {
                            this._next = this._tupleIterator.next();
                        }
                        else
                        {
                            this._tupleIterator = null;
                            this._next = null;
                        }

                    if (this._nextPage == this._numPages)
                        break;
                }
            }

            return this._next;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException
        {
            if (!this._open)
            {
                this._open = true;
                this._nextPage = 0;
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException
        {
            this.close();
            this.open();
        }

        @Override
        public void close()
        {
            this._tupleIterator = null;
            this._next = null;
            this._open = false;
        }
    }
}
