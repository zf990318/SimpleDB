package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc td;
	private int numPage;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
	}

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	Page newPage;
		byte[] table = new byte[BufferPool.getPageSize()];
		try {
			RandomAccessFile file = new RandomAccessFile(getFile(), "r");
			int index = pid.pageNumber() * BufferPool.getPageSize();
			file.seek(index);
			file.read(table,0,table.length);
			newPage = new HeapPage((HeapPageId) pid, table);
		}catch(Exception e) {
    		 throw new IllegalArgumentException();
		}
        return newPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
		try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
			byte[] data = page.getPageData();
			raf.seek((long) page.getId().pageNumber() * BufferPool.getPageSize());
			raf.write(data);
			raf.close();
		}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil((double)this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
		ArrayList<Page> affectedPageArrayList = new ArrayList<>();

		for (int i = 0; i < numPages(); i++) {
			HeapPageId heapPID = new HeapPageId(getId(), i);
			HeapPage hPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPID, Permissions.READ_WRITE);
			if (hPage.getNumEmptySlots() != 0) {
				hPage.insertTuple(t);
				hPage.markDirty(true, tid);
				affectedPageArrayList.add(hPage);
				break;
			}
		}

		//if page is full
		if (affectedPageArrayList.size() == 0) {
			//create a new empty page
			HeapPageId newPID = new HeapPageId(getId(), numPages());
			HeapPage blankPage = new HeapPage(newPID, HeapPage.createEmptyPageData());
			numPage++;
			//write it into disk
			writePage(blankPage);
			//access through BufferPool
			HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, newPID, Permissions.READ_WRITE);
			newPage.insertTuple(t);
			newPage.markDirty(true, tid);
			affectedPageArrayList.add(newPage);
		}
		return affectedPageArrayList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
		HeapPage hPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), t.getRecordId().getPageId().pageNumber()), Permissions.READ_WRITE);
		hPage.deleteTuple(t);

		ArrayList<Page> pages = new ArrayList<>();
		pages.add(hPage);

		return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileiterator(tid);
    }
    
    public class HeapFileiterator implements DbFileIterator {
    	
    	private int pageIndex;
    	private TransactionId tid;
    	private Iterator<Tuple> iteratorT;
    	
		public HeapFileiterator(TransactionId tid) {
             this.tid = tid;
         }
    	
    	public Iterator<Tuple> tuplesInPage(HeapPageId pid) {
			HeapPage page = null;
			try {
				page =  (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
			} catch (TransactionAbortedException | DbException e) {
				e.printStackTrace();
			}
			assert page != null;
			return page.iterator();
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			pageIndex = 0;
			HeapPageId pid = new HeapPageId(getId(), pageIndex);
			iteratorT = tuplesInPage(pid);
			
		}

		

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			
			if(iteratorT == null) {
				return false;
			}
			
			if (iteratorT.hasNext()) {
                return true;
            }
			
			 if (pageIndex < numPages() - 1) {
	                pageIndex += 1;
	                HeapPageId pid = new HeapPageId(getId(), pageIndex);
	                iteratorT = tuplesInPage(pid);
	                return iteratorT.hasNext();
	            } else {
	            	return false;
	            }
    }

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			if(!hasNext()) {
				throw new NoSuchElementException("There is no tuple left");
			}
			else {
				return iteratorT.next();
			}
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			
			open();
			
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			
			pageIndex =0;
			iteratorT = null;
			
		}
    	
    }
}