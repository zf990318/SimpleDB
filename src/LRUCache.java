package simpledb;

import java.util.Iterator;
import java.util.LinkedHashMap;

public class LRUCache<PageId, Page> {

    private int capacity;
    private LinkedHashMap<PageId, Page> map;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.map = new LinkedHashMap<PageId, Page>();
    }


    public boolean containsKey(PageId pageId){
        return map.containsKey(pageId);
    }


    public synchronized Page get(PageId pageId) {
        Page value = this.map.get(pageId);
        if (value != null) {
            this.put(pageId, value);
        }
        return value;
    }


    public Page evict(){
        Iterator<PageId> it = this.map.keySet().iterator();
        PageId pageId = it.next();
        Page page = map.get(pageId);
        it.remove();
        return page;
    }


    public void put(PageId pageId, Page page) {
        if (this.map.containsKey(pageId)) {
            this.map.remove(pageId);
        } else if (this.map.size() == this.capacity) {
            this.evict();
        }
        map.put(pageId, page);
    }


    public int size(){
        return map.size();
    }

    public Iterator<PageId> keySet(){
        return this.map.keySet().iterator();
    }


    public Iterator<Page> values(){
        return this.map.values().iterator();
    }


    public synchronized void remove(PageId pageId){
        this.map.remove(pageId);
    }
    
    

}
