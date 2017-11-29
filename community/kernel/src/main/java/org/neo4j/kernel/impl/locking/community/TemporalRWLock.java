package org.neo4j.kernel.impl.locking.community;

import org.neo4j.kernel.impl.locking.LockType;
import org.neo4j.kernel.impl.util.ArrayMap;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

import static java.lang.Thread.interrupted;
import static org.neo4j.kernel.impl.locking.LockType.READ;
import static org.neo4j.kernel.impl.locking.LockType.WRITE;

/**
 * Created by song on 2017/7/22 0022.
 * The reason we use two separate
 */
public class TemporalRWLock extends RWLock
{
    TemporalRWLock(Object resource, RagManager ragManager)
    {
        this.ragManager = ragManager;
        this.resource = resource;
    }

    @Override
    public synchronized void releaseTemporalReadLock( Object tx )
    {
        TxLockElement tle = getLockElement( tx );


//      totalReadCount--;
//      tle.readCount--;
        this.readIntervalList.remove( tle.readInterval );
        if ( tle.isFree() )
        {
            ragManager.lockReleased( this, tx );
            if(tle.hasNoRequests())
            {
                txLockElementMap.remove( tx );
            }
        }
        if ( waitingThreadList.size() > 0 )
        {
            LockRequest lockRequest = waitingThreadList.getLast();

            if ( lockRequest.lockType == WRITE )
            {
                // this one is tricky...
                // if readCount > 0 we either have to find a waiting read lock
                // in the queue or a waiting write lock that has all read
                // locks, if none of these are found it means that there
                // is a (are) thread(s) that will release read lock(s) in the
                // near future...
                if ( null == this.WriteThreshold &&
                        !this.readIntervalList.unionExept(lockRequest.element.readInterval,  new TimeInterval( lockRequest.writeWaitTime,lockRequest.writeWaitTime ) ) )
                {
                    // found a write lock with all read locks
                    waitingThreadList.removeLast();
                    lockRequest.waitingThread.interrupt();
                }
                else
                {
                    ListIterator<LockRequest> listItr = waitingThreadList.listIterator(
                            waitingThreadList.lastIndexOf( lockRequest ) );
                    // hm am I doing the first all over again?
                    // think I am if cursor is at lastIndex + 0.5 oh well...
                    while ( listItr.hasPrevious() )
                    {
                        lockRequest = listItr.previous();
                        if ( lockRequest.lockType == WRITE && ( null == this.WriteThreshold &&
                                !this.readIntervalList.unionExept(lockRequest.element.readInterval,  new TimeInterval( lockRequest.writeWaitTime,lockRequest.writeWaitTime ) ) ) )
                        {
                            // found a write lock with all read locks
                            listItr.remove();
                            lockRequest.waitingThread.interrupt();
                            break;
                        }
                        else if ( lockRequest.lockType == READ )
                        {
                            // found a read lock, let it do the job...
                            listItr.remove();
                            lockRequest.waitingThread.interrupt();
                        }
                    }
                }
            }
            else
            {
                // some thread may have the write lock and released a read lock
                // if writeCount is down to zero we can interrupt the waiting
                // read lock
                if ( this.WriteThreshold == null )
                {
                    waitingThreadList.removeLast();
                    lockRequest.waitingThread.interrupt();
                }
            }
        }
    }

    @Override
    public synchronized void releaseTemporalWriteLock( Object tx )
    {
        TxLockElement tle = getLockElement( tx );

//      totalWriteCount--;
//      tle.writeCount--;
        tle.writeThreadshold = null;
        this.WriteThreshold = null;
        if ( tle.isFree() )
        {
            ragManager.lockReleased( this, tx );
            if(tle.hasNoRequests())
            {
                txLockElementMap.remove(tx);
            }
        }

        // the threads in the waitingList cannot be currentThread
        // so we only have to wake other elements if writeCount is down to zero
        // (that is: If writeCount > 0 a waiting thread in the queue cannot be
        // the thread that holds the write locks because then it would never
        // have been put into wait mode)
        if ( this.WriteThreshold == null && waitingThreadList.size() > 0 )
        {
            // wake elements in queue until a write lock is found or queue is
            // empty
            do
            {
                LockRequest lockRequest = waitingThreadList.removeLast();
                lockRequest.waitingThread.interrupt();
                if ( lockRequest.lockType == WRITE )
                {
                    break;
                }
            }
            while ( waitingThreadList.size() > 0 );
        }
    }

    @Override
    public synchronized void releaseTemporalReadLock( Object tx, int start, int end )
    {
        TxLockElement tle = getLockElement( tx );


//        totalReadCount--;
//        tle.readCount--;
        tle.readInterval.remove( new TimeInterval( start, end ) );
        this.readIntervalList.remove( new TimeInterval( start, end ) );
        if ( tle.isFree() )
        {
            ragManager.lockReleased( this, tx );
            if ( tle.hasNoRequests() )
            {
                txLockElementMap.remove( tx );
            }
        }
        if ( waitingThreadList.size() > 0 )
        {
            LockRequest lockRequest = waitingThreadList.getLast();

            if ( lockRequest.lockType == WRITE )
            {
                // this one is tricky...
                // if readCount > 0 we either have to find a waiting read lock
                // in the queue or a waiting write lock that has all read
                // locks, if none of these are found it means that there
                // is a (are) thread(s) that will release read lock(s) in the
                // near future...
                if ( null == this.WriteThreshold &&
                        !this.readIntervalList.unionExept(lockRequest.element.readInterval,  new TimeInterval( lockRequest.writeWaitTime,lockRequest.writeWaitTime ) ) )
                {
                    // found a write lock with all read locks
                    waitingThreadList.removeLast();
                    lockRequest.waitingThread.interrupt();
                }
                else
                {
                    ListIterator<LockRequest> listItr = waitingThreadList.listIterator(
                            waitingThreadList.lastIndexOf( lockRequest ) );
                    // hm am I doing the first all over again?
                    // think I am if cursor is at lastIndex + 0.5 oh well...
                    while ( listItr.hasPrevious() )
                    {
                        lockRequest = listItr.previous();
                        if ( lockRequest.lockType == WRITE && ( null == this.WriteThreshold &&
                                !this.readIntervalList.unionExept(lockRequest.element.readInterval,  new TimeInterval( lockRequest.writeWaitTime,lockRequest.writeWaitTime ) ) ) )
                        {
                            // found a write lock with all read locks
                            listItr.remove();
                            lockRequest.waitingThread.interrupt();
                            break;
                        }
                        else if ( lockRequest.lockType == READ )
                        {
                            // found a read lock, let it do the job...
                            listItr.remove();
                            lockRequest.waitingThread.interrupt();
                        }
                    }
                }
            }
            else
            {
                // some thread may have the write lock and released a read lock
                // if writeCount is down to zero we can interrupt the waiting
                // read lock
                if ( this.WriteThreshold == null )
                {
                    waitingThreadList.removeLast();
                    lockRequest.waitingThread.interrupt();
                }
            }
        }
    }

    @Override
    public synchronized void releaseTemporalWriteLock( Object tx, int time )
    {
        TxLockElement tle = getLockElement( tx );


//        totalWriteCount--;
//        tle.writeCount--;
        tle.writeThreadshold = null;
        this.WriteThreshold = null;
        if ( tle.isFree() )
        {
            ragManager.lockReleased( this, tx );
            if ( tle.hasNoRequests() )
            {
                txLockElementMap.remove( tx );
            }
        }

        // the threads in the waitingList cannot be currentThread
        // so we only have to wake other elements if writeCount is down to zero
        // (that is: If writeCount > 0 a waiting thread in the queue cannot be
        // the thread that holds the write locks because then it would never
        // have been put into wait mode)
        if ( this.WriteThreshold == null && waitingThreadList.size() > 0 )
        {
            // wake elements in queue until a write lock is found or queue is
            // empty
            do
            {
                LockRequest lockRequest = waitingThreadList.removeLast();
                lockRequest.waitingThread.interrupt();
                if ( lockRequest.lockType == WRITE )
                {
                    break;
                }
            }
            while ( waitingThreadList.size() > 0 );
        }
    }

    @Override
    public synchronized boolean tryAcquireTemporalReadLock( Object tx, int start, int end )
    {
        return acquireTemporalReadLock(tx, start, end);
    }

    @Override
    public synchronized boolean tryAcquireTemporalWriteLock( Object tx, int time )
    {
        return acquireTemporalWriteLock(tx, time);
    }

    @Override
    public synchronized boolean acquireTemporalWriteLock( Object tx, int time )
    {
        TxLockElement tle = getOrCreateLockElement( tx );
        LockRequest lockRequest = null;
        boolean shouldAddWait = true;
        try
        {
            tle.incrementRequests();

            Thread currentThread = Thread.currentThread();

            //Conditions to wait: current tx not has write lock, other tx has write lock or OTHER has read lock union the time
            while ( !tle.isTerminated() && (tle.writeThreadshold == null && ( (  this.WriteThreshold != null )
                    || ( this.readIntervalList.unionExept( tle.readInterval, new TimeInterval( time, time ) ) ) ) ))
            {
                ragManager.checkWaitOn( this, tx );

                if (shouldAddWait)
                {
                    lockRequest = new LockRequest( tle, WRITE, currentThread, time);
                    waitingThreadList.addFirst( lockRequest );
                }

                try
                {
                    wait();
                    shouldAddWait = false;
                }
                catch ( InterruptedException e )
                {
                    interrupted();

                    shouldAddWait = true;
                }
                ragManager.stopWaitOn( this, tx );
            }

            if ( !tle.isTerminated() )
            {
                registerWriteLockAcquired( tx, tle, time );
                return true;
            } else {
                // in case if lock element was interrupted and it was never register before
                // we need to clean it from lock element map
                // if it was register before it will be cleaned up during standard lock release call
                if ( tle.requests == 1 && tle.isFree() )
                {
                    txLockElementMap.remove( tx );
                }
                return false;
            }
        }
        finally
        {
            cleanupWaitingListRequests( lockRequest, tle, shouldAddWait );
            // for cases when spurious wake up was the reason why we waked up, but also there
            // was an interruption as described at 17.2 just clearing interruption flag
            interrupted();
            // if deadlocked, remove marking so lock is removed when empty
            tle.decrementRequests();
            marked--;
        }
    }

    @Override
    public synchronized boolean acquireTemporalReadLock( Object tx, int start, int end )
    {
        TxLockElement tle = getOrCreateLockElement( tx );

        LockRequest lockRequest = null;
        boolean shouldAddWait = true;
        try
        {
            tle.incrementRequests();

            Thread currentThread = Thread.currentThread();

            //Conditions to wait: current tx not has write lock, other tx has write lock and end time union the write lock time
            while ( !tle.isTerminated() && (tle.writeThreadshold == null && ( this.WriteThreshold != null && end >= this.WriteThreshold ) ))
            {
                ragManager.checkWaitOn( this, tx );

                if (shouldAddWait)
                {
                    lockRequest = new LockRequest( tle, READ, currentThread, -1);
                    waitingThreadList.addFirst(lockRequest);
                }

                try
                {
                    wait();
                    shouldAddWait = false;
                }
                catch ( InterruptedException e )
                {
                    interrupted();

                    shouldAddWait = true;
                }
                ragManager.stopWaitOn( this, tx );
            }

            if ( !tle.isTerminated() )
            {
                registerReadLockAcquired( tx, tle, start, end );
                return true;
            } else {
                // in case if lock element was interrupted and it was never register before
                // we need to clean it from lock element map
                // if it was register before it will be cleaned up during standard lock release call
                if ( tle.requests == 1 && tle.isFree() )
                {
                    txLockElementMap.remove( tx );
                }
                return false;
            }
        }
        finally
        {
            cleanupWaitingListRequests( lockRequest, tle, shouldAddWait );
            interrupted();
            // if deadlocked, remove marking so lock is removed when empty
            tle.decrementRequests();
            marked--;
        }
    }

    private Integer WriteThreshold = null;
    private TimeIntervalList readIntervalList = new TimeIntervalList();

    private final Object resource; // the resource this RWLock locks
    private final LinkedList<LockRequest> waitingThreadList = new LinkedList<>();
    private final ArrayMap<Object,TxLockElement> txLockElementMap = new ArrayMap<>( (byte)5, false, true );
    private final RagManager ragManager;

    // access to these is guarded by synchronized blocks
    private int totalReadCount;
    private int totalWriteCount;
    private int marked; // synch helper in LockManager

    // keeps track of a transactions read and write lock count on this RWLock
    private static class TxLockElement
    {
        private final Object tx;

        // access to these is guarded by synchronized blocks
        private TimeIntervalList readInterval = new TimeIntervalList();
        private Integer writeThreadshold = null;
        private boolean terminated;
        public int requests=0;

        TxLockElement( Object tx )
        {
            this.tx = tx;
        }

        boolean isFree()
        {
            return readInterval.size() == 0 && writeThreadshold == null;
        }

        public boolean isTerminated()
        {
            return terminated;
        }

        public void setTerminated( boolean terminated )
        {
            this.terminated = terminated;
        }
        void incrementRequests()
        {
            requests++;
        }

        void decrementRequests()
        {
            requests--;
        }

        boolean hasNoRequests()
        {
            return requests == 0;
        }
    }

    // keeps track of what type of lock a thread is waiting for
    private static class LockRequest
    {
        private final TxLockElement element;
        private final LockType lockType;
        private final Thread waitingThread;
        private final long since = System.currentTimeMillis();
        private final int writeWaitTime;

        LockRequest(TxLockElement element, LockType lockType, Thread thread, int writeWaitTime )
        {
            this.element = element;
            this.lockType = lockType;
            this.waitingThread = thread;
            if( lockType == WRITE )
                this.writeWaitTime = writeWaitTime;
            else
                this.writeWaitTime = -1;
        }
    }

    public Object resource()
    {
        return resource;
    }

    public synchronized void mark()
    {
        this.marked++;
    }

    public synchronized boolean isMarked()
    {
        return marked > 0;
    }



    public int getWriteCount()
    {
        return totalWriteCount;
    }

    public int getReadCount()
    {
        return totalReadCount;
    }

    public synchronized int getWaitingThreadsCount()
    {
        return waitingThreadList.size();
    }

    public synchronized String describe()
    {
        StringBuilder sb = new StringBuilder( this.toString() );
        sb.append(" Total lock count: readCount=").append(totalReadCount)
                .append(" writeCount=").append(totalWriteCount)
                .append(" for ").append(resource).append("\n")
                .append( "Waiting list:" + "\n" );
        Iterator<LockRequest> wElements = waitingThreadList.iterator();
        while ( wElements.hasNext() )
        {
            LockRequest we = wElements.next();
            sb.append("[").append(we.waitingThread).append("(").append(we.element.readInterval).append("r,").append(we.element.writeThreadshold).append("w),").append(we.lockType).append("]\n");
            if ( wElements.hasNext() )
            {
                sb.append( "," );
            }
        }

        sb.append( "Locking transactions:\n" );
        Iterator<TxLockElement> lElements = txLockElementMap.values().iterator();
        while ( lElements.hasNext() )
        {
            TxLockElement tle = lElements.next();
            sb.append("").append(tle.tx).append("(").append(tle.readInterval).append("r,").append(tle.writeThreadshold).append("w)\n");
        }
        return sb.toString();
    }

    public synchronized long maxWaitTime()
    {
        long max = 0L;
        for ( LockRequest thread : waitingThreadList )
        {
            if ( thread.since < max )
            {
                max = thread.since;
            }
        }
        return System.currentTimeMillis() - max;
    }

    private void registerReadLockAcquired( Object tx, TxLockElement tle, int start, int end )
    {
        registerLockAcquired( tx, tle );
//        totalReadCount++;
        tle.readInterval.insert( new TimeInterval( start, end ) );
        this.readIntervalList.insert( new TimeInterval( start, end ) );
    }

    private void registerWriteLockAcquired( Object tx, TxLockElement tle, int time )
    {
        registerLockAcquired( tx, tle );
//        totalWriteCount++;
//        tle.writeCount++;
        tle.writeThreadshold = time;
        this.WriteThreshold = time;
    }

    private void registerLockAcquired( Object tx, TxLockElement tle )
    {
        if ( tle.isFree() )
        {
            ragManager.lockAcquired( this, tx );
        }
    }

    private TxLockElement getLockElement( Object tx )
    {
        TxLockElement tle = txLockElementMap.get( tx );
        if ( tle == null )
        {
            throw new LockNotFoundException( "No transaction lock element found for " + tx );
        }
        return tle;
    }

    private void assertTransaction( Object tx )
    {
        if ( tx == null )
        {
            throw new IllegalArgumentException();
        }
    }

    private TxLockElement getOrCreateLockElement( Object tx )
    {
        assertTransaction( tx );
        TxLockElement tle = txLockElementMap.get( tx );
        if ( tle == null )
        {
            txLockElementMap.put( tx, tle = new TxLockElement( tx ) );
        }
        return tle;
    }

    public synchronized Object getTxLockElementCount()
    {
        return txLockElementMap.size();
    }

    private void cleanupWaitingListRequests( LockRequest lockRequest, TxLockElement lockElement,
                                             boolean addLockRequest )
    {
        if ( lockRequest != null && (lockElement.isTerminated() || !addLockRequest) )
        {
            waitingThreadList.remove( lockRequest );
        }
    }
}
