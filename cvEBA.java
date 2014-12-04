package cveb;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicLong;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class CvEBA<E> {

    private final ArrayHolder rootAH;
    private final int dimension, dimSize;

    private class ArrayHolder {

        private final AtomicReferenceArray<ArrayHolder> array;
        protected final AtomicLong summary;
        protected final ReadWriteLock lock;

        public ArrayHolder() {
            summary = new AtomicLong(0);
            array = new AtomicReferenceArray<>(dimSize);
            lock = new ReentrantReadWriteLock();
        }
    }

    private class LeafArrayHolder extends ArrayHolder {

        private final AtomicReferenceArray<String> data;

        public LeafArrayHolder() {
            data = new AtomicReferenceArray<>(dimSize);
        }
    }

    public CvEBA(int dim, int ds) {
        dimension = dim;
        dimSize = ds;
        rootAH = new ArrayHolder();

    }

    public void insert(int key, String value) {
        ArrayHolder currentAH = rootAH;
        ArrayHolder prevousAH = null;
        LeafArrayHolder leafAH;
        int currentLevel = 0, i = key, lp;

        if (Math.pow(dimSize, dimension) < key) {
            System.out.println("key out of range" + key);
        }
        //get the parent node of the leaf node
        while (currentLevel < dimension - 2) {
            lp = i / (int) Math.pow(dimSize, (dimension - currentLevel - 1));
            i = i - lp * (int) Math.pow(dimSize, dimension - currentLevel - 1);
            currentAH.lock.readLock().lock();
            if(prevousAH !=null)
                prevousAH.lock.readLock().unlock();
            prevousAH = currentAH;
            currentAH.summary.set(1L << lp | currentAH.summary.get());

            if (currentAH.array.get(lp) == null) {
                currentAH.array.set(lp, new ArrayHolder());
            }
            currentAH = (ArrayHolder) currentAH.array.get(lp);
            currentLevel++;
        }
        //get the leaf node
        lp = i / (int) Math.pow(dimSize, (dimension - currentLevel - 1));
        i = i - lp * (int) Math.pow(dimSize, dimension - currentLevel - 1);
        currentAH.lock.readLock().lock();
        if(prevousAH !=null)
            prevousAH.lock.readLock().unlock();
        prevousAH = currentAH;
        currentAH.summary.set(1L << lp | currentAH.summary.get());
        if (currentAH.array.get(lp) == null) {
            leafAH = new LeafArrayHolder();
            currentAH.array.set(lp, leafAH);
        } else {
            leafAH = (LeafArrayHolder) currentAH.array.get(lp);
        }
        //modify the leaf node
        currentLevel++;
        lp = i / (int) Math.pow(dimSize, (dimension - currentLevel - 1));
        leafAH.lock.readLock().lock();
        try {
            prevousAH.lock.readLock().unlock();
            leafAH.summary.set(1L << lp | leafAH.summary.get());
            leafAH.data.set(lp, value);
        } finally {
            leafAH.lock.readLock().unlock();
        }
    }

    public String get(int key) {
        ArrayHolder currentAH = rootAH;
        LeafArrayHolder leafAH;
        int currentLevel = 0, i = key, lp;

        if (Math.pow(dimSize, dimension) < key) {
            System.out.println("key out of range" + key);
            return null;
        }
        while (currentLevel < dimension - 2) {
            lp = i / (int) Math.pow(dimSize, (dimension - currentLevel - 1));
            i = i - lp * (int) Math.pow(dimSize, dimension - currentLevel - 1);
            if ((currentAH.summary.get() & (1L << lp)) == 0) {
                return null;
            }
            currentAH = (ArrayHolder) currentAH.array.get(lp);
            currentLevel++;
        }
        //get the leaf node
        lp = i / (int) Math.pow(dimSize, (dimension - currentLevel - 1));
        i = i - lp * (int) Math.pow(dimSize, dimension - currentLevel - 1);
        if ((currentAH.summary.get() & (1L << lp)) == 0) {
            return null;
        }
        leafAH = (LeafArrayHolder) currentAH.array.get(lp);
        currentLevel++;
        lp = i / (int) Math.pow(dimSize, (dimension - currentLevel - 1));
        if((leafAH.summary.get() & (1L<<lp))==0)
            return null;
        return leafAH.data.get(lp);
    }

    public void delete(int key) {
        ArrayHolder currentAH = rootAH;
        LeafArrayHolder leafAH;
        int currentLevel = 0, i = key, lp;

        ArrayList<ArrayHolder> al = new ArrayList<>(dimension - 2);
        int[] pos = new int[dimension - 2];

        while (currentLevel < dimension - 2) {
            lp = i / (int) Math.pow(dimSize, (dimension - currentLevel - 1));
            i = i - lp * (int) Math.pow(dimSize, dimension - currentLevel - 1);
            if ((currentAH.summary.get() & (1L << lp)) == 0) {
                return;
            }
            pos[currentLevel] = lp;
            al.add(currentAH);
            currentAH = (ArrayHolder) currentAH.array.get(lp);
            currentLevel++;
        }
        lp = i / (int) Math.pow(dimSize, (dimension - currentLevel - 1));
        i = i - lp * (int) Math.pow(dimSize, dimension - currentLevel - 1);
        currentLevel++;
        if ((currentAH.summary.get() & (1L << lp)) == 0) {
            return;
        }
        leafAH = (LeafArrayHolder) currentAH.array.get(lp);
        lp = i / (int) Math.pow(dimSize, (dimension - currentLevel - 1));
        
        if ((leafAH.summary.get() & (1L << lp)) == 0) {
            return;
        }
        try{
            currentAH.lock.writeLock().lock();
            leafAH.lock.writeLock().lock();
            try {
                leafAH.data.set(lp, null);
                leafAH.summary.set(leafAH.summary.get() & ~(1L << lp));
            } finally {
                leafAH.lock.writeLock().unlock();
            }
            if (leafAH.summary.get() != 0) {
                currentAH.lock.writeLock().unlock();
                return;
            }
            currentAH.summary.set(currentAH.summary.get() & ~(1L << lp));
            if (currentAH.summary.get() != 0) {
                currentAH.lock.writeLock().unlock();
                return;
            }
        }
        finally{
            //leafAH.lock.writeLock().unlock();
            //currentAH.lock.writeLock().unlock();
        }
        int p = al.size() - 1;
        while (p >= 0) {
            currentAH = al.get(p);
            currentAH.lock.writeLock().lock();
            lp = pos[p];
            currentAH.summary.set(currentAH.summary.get() & ~(1L << lp));
            if (currentAH.summary.get() != 0) {
                currentAH.lock.writeLock().unlock();
                return;
            }
            p--;
        }
        currentAH.lock.writeLock().unlock();
    }

    static void memusage(){
        //ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        System.out.println("Heap : "+ ManagementFactory.getMemoryMXBean().getHeapMemoryUsage());
        System.out.println("NonHeap : "+ ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage());
        List<MemoryPoolMXBean> beans = ManagementFactory.getMemoryPoolMXBeans();
        beans.stream().forEach((bean) -> {
            System.out.println(bean.getName()+"\t"+ bean.getUsage());
        });

        ManagementFactory.getGarbageCollectorMXBeans().stream().forEach((bean) -> {
            System.out.println(bean.getName() +"\t"+ bean.getCollectionCount()+"\t"+ bean.getCollectionTime());
        });
    }
}
class InserterThread extends Thread{
    private final int init; 
    private final int size;
    private final CvEBA rootAH;
    public InserterThread(CvEBA rah, int in, int s){
        rootAH = rah;
        init = in;
        size = s;
    }
    @Override
    public void run(){
        for(int i = init; i<size;i++)
            rootAH.insert(i, "Hello : " + i);       
    }
}
class DeleterThread extends Thread{
    private final int init; 
    private final int size;
    private final CvEBA rootAH;
    public DeleterThread(CvEBA rah, int in, int s){
        rootAH = rah;
        init = in;
        size = s;
    }
    @Override
    public void run(){
        for(int i = init; i<size;i++)
            rootAH.delete(i);       
    }
}
class GetterThread extends Thread {
    private final int init; 
    private final int size;
    private final CvEBA rootAH;
    public GetterThread(CvEBA rah, int in, int s){
        rootAH = rah;
        init = in;
        size = s;
    }
    @Override
    public void run(){
        for(int i = init; i<size;i++)
            rootAH.get(i);
    }
}

class CvEB {

    public static void main(String[] args) throws IOException{
        
        CvEBA cvEB = new CvEBA(3, 16);
        InserterThread i1 = new InserterThread(cvEB, 0, 4096);
        InserterThread i2 = new InserterThread(cvEB, 2048, 4096);
        
        GetterThread g1 = new GetterThread(cvEB, 0, 4096);
        GetterThread g2 = new GetterThread(cvEB, 2048, 4096);
        
        DeleterThread d1 = new DeleterThread(cvEB, 0, 4096);
        DeleterThread d2 = new DeleterThread(cvEB, 2048, 4096);
       
        long start, end;
        start = System.nanoTime();
        i1.start();
        i2.start();
        end = System.nanoTime();
        
        long insert = end - start;
        start = System.nanoTime();
        g1.start();
        g2.start();
        end = System.nanoTime();
        long get = end - start;

        CvEBA.memusage();
       
        start = System.nanoTime();
        d1.start();
        d2.start();
        end = System.nanoTime();
        long remove = end - start;
        CvEBA.memusage();
        for (int j= 0; j < 4096;j++)
          System.out.println(j + " : " + cvEB.get(j));
        System.out.println(insert + "\n" + get + "\n" + remove);
    }
}

