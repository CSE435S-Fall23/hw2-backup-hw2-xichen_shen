package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
/*
 * Name1: Xi Chen
 * Name2: Jacob Shen
 */

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 *
 * @author Sam Madden modified by Doug Shook
 */

public class HeapFile {

    public static final int PAGE_SIZE = 4096;
    private File file;
    private TupleDesc tupleDesc;
    private Integer tableId;
    private int pages;

    /**
     * Creates a new heap file in the given location that can accept tuples of the given type
     *
     * @param f     location of the heap file
     * @param types type of tuples contained in the file
     */
    public HeapFile(File f, TupleDesc types) {
        //your code here
        this.tableId = this.hashCode();
        this.file = f;
        this.tupleDesc = types;
        this.pages = (int) Math.ceil(f.length() / PAGE_SIZE);
    }

    public File getFile() {
        //your code here
        return this.file;
    }

    public TupleDesc getTupleDesc() {
        //your code here
        return this.tupleDesc;
    }

    /**
     * Creates a HeapPage object representing the page at the given page number.
     * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
     * should be used here.
     *
     * @param id the page number to be retrieved
     * @return a HeapPage at the given page number
     */
    public HeapPage readPage(int id) {
        //your code here
        long currentLimit = id * PAGE_SIZE;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(currentLimit);
            byte[] bytes = new byte[PAGE_SIZE];
            raf.read(bytes);
            return new HeapPage(id, bytes, getId());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Returns a unique id number for this heap file. Consider using
     * the hash of the File itself.
     *
     * @return
     */
    public int getId() {
        //your code here
        return tableId;
    }

    /**
     * Writes the given HeapPage to disk. Because of the need to seek through the file,
     * a RandomAccessFile object should be used in this method.
     *
     * @param p the page to write to disk
     */
    public void writePage(HeapPage p) {
        //your code here
        int currentLimit = p.getId() * PAGE_SIZE;
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(currentLimit);
            raf.write(p.getPageData());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Adds a tuple. This method must first find a page with an open slot, creating a new page
     * if all others are full. It then passes the tuple to this page to be stored. It then writes
     * the page to disk (see writePage)
     *
     * @param t The tuple to be stored
     * @return The HeapPage that contains the tuple
     */
    public HeapPage addTuple(Tuple t) {
        //your code here
        try {
            for (int i = 0; i < pages; i++) {
                try {
                    HeapPage hp = readPage(i);
                    hp.addTuple(t);
                    writePage(hp);
                    return hp;
                } catch (Exception e) {

                }
            }
            HeapPage hp = new HeapPage(pages++, new byte[PAGE_SIZE], getId());
            hp.addTuple(t);
            writePage(hp);
            return hp;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * This method will examine the tuple to find out where it is stored, then delete it
     * from the proper HeapPage. It then writes the modified page to disk.
     *
     * @param t the Tuple to be deleted
     */
    public void deleteTuple(Tuple t) {
        //your code here
        try {
            int pageIndex = t.getPid();
            HeapPage hp = readPage(pageIndex);
            hp.deleteTuple(t);
            writePage(hp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns an ArrayList containing all of the tuples in this HeapFile. It must
     * access each HeapPage to do this (see iterator() in HeapPage)
     *
     * @return
     */
    public ArrayList<Tuple> getAllTuples() {
        //your code here
        ArrayList<Tuple> arrayList = new ArrayList<>();
        try {
            for (int i = 0; i < pages; i++) {
                HeapPage heapPage = readPage(i);
                heapPage.iterator().forEachRemaining(arrayList::add);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return arrayList;
    }

    /**
     * Computes and returns the total number of pages contained in this HeapFile
     *
     * @return the number of pages
     */
    public int getNumPages() {
        //your code here
        return pages;
    }
}