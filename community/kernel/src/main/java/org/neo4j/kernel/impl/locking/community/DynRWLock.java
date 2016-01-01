package org.neo4j.kernel.impl.locking.community;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.interrupted;
import static org.neo4j.kernel.impl.locking.LockType.READ;
import static org.neo4j.kernel.impl.locking.LockType.WRITE;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.LockType;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.StringLogger.LineLogger;
import org.neo4j.kernel.impl.util.TimeInterval;
import org.neo4j.kernel.impl.util.collection.TimeIntervalList;

public class DynRWLock extends RWLock
{
    
    DynRWLock( Object resource, RagManager ragManager )
    {
        super( resource, ragManager );
        this.resource = resource;
        this.ragManager = ragManager;
    }

    synchronized void releaseReadLock( LockTransaction tx, int start, int end )
    {
        TxLockElement tle = getLockElement( tx );

        if ( tle.readCount == 0 )
        {
            throw new LockNotFoundException( "" + tx + " don't have readLock" );
        }

//        totalReadCount--;
//        tle.readCount--;
        tle.readCount--;
        this.readIntervalList.remove( new TimeInterval( start, end ) );
        if ( tle.isFree() )
        {
            txLockElementMap.remove( tx );
            ragManager.lockReleased( this, tx );
        }
        if ( waitingThreadList.size() > 0 )
        {
            WaitElement we = waitingThreadList.getLast();

            if ( we.lockType == LockType.WRITE )
            {
                // this one is tricky...
                // if readCount > 0 we either have to find a waiting read lock
                // in the queue or a waiting write lock that has all read
                // locks, if none of these are found it means that there
                // is a (are) thread(s) that will release read lock(s) in the
                // near future...
                if ( !this.readIntervalList.union(  new TimeInterval( we.element.writeThreadshold,we.element.writeThreadshold ) ) )
                {
                    // found a write lock with all read locks
                    waitingThreadList.removeLast();
                    if ( !we.element.movedOn )
                    {
                        we.waitingThread.interrupt();
                    }
                }
                else
                {
                    ListIterator<WaitElement> listItr = waitingThreadList.listIterator(
                            waitingThreadList.lastIndexOf( we ) );
                    // hm am I doing the first all over again?
                    // think I am if cursor is at lastIndex + 0.5 oh well...
                    while ( listItr.hasPrevious() )
                    {
                        we = listItr.previous();
                        if ( we.lockType == LockType.WRITE && ( !this.readIntervalList.union(  new TimeInterval( we.element.writeThreadshold,we.element.writeThreadshold ) ) ) )
                        {
                            // found a write lock with all read locks
                            listItr.remove();
                            if ( !we.element.movedOn )
                            {
                                we.waitingThread.interrupt();
                                // ----
                                break;
                            }
                        }
                        else if ( we.lockType == LockType.READ )
                        {
                            // found a read lock, let it do the job...
                            listItr.remove();
                            if ( !we.element.movedOn )
                            {
                                we.waitingThread.interrupt();
                            }
                        }
                    }
                }
            }
            else
            {
                // some thread may have the write lock and released a read lock
                // if writeCount is down to zero we can interrupt the waiting
                // read lock
                if ( totalWriteCount == 0 )
                {
                    waitingThreadList.removeLast();
                    if ( !we.element.movedOn )
                    {
                        we.waitingThread.interrupt();
                    }
                }
            }
        }
    }

    synchronized void releaseWriteLock( LockTransaction tx, int time )
    {
        TxLockElement tle = getLockElement( tx );

        if ( tle.writeCount == 0 )
        {
            throw new LockNotFoundException( "" + tx + " don't have writeLock" );
        }

//        totalWriteCount--;
//        tle.writeCount--;
        tle.writeThreadshold = null;
        this.WriteThreshold = null;
        if ( tle.isFree() )
        {
            txLockElementMap.remove( tx );
            ragManager.lockReleased( this, tx );
        }

        // the threads in the waitingList cannot be currentThread
        // so we only have to wake other elements if writeCount is down to zero
        // (that is: If writeCount > 0 a waiting thread in the queue cannot be
        // the thread that holds the write locks because then it would never
        // have been put into wait mode)
        if ( totalWriteCount == 0 && waitingThreadList.size() > 0 )
        {
            // wake elements in queue until a write lock is found or queue is
            // empty
            do
            {
                WaitElement we = waitingThreadList.removeLast();
                if ( !we.element.movedOn )
                {
                    we.waitingThread.interrupt();
                    if ( we.lockType == LockType.WRITE )
                    {
                        break;
                    }
                }
            }
            while ( waitingThreadList.size() > 0 );
        }
    }

    synchronized boolean tryAcquireReadLock( LockTransaction tx, int start, int end )
    {
        TxLockElement tle = getOrCreateLockElement( tx );

        try
        {
            tle.movedOn = false;
            if ( tle.writeThreadshold == null && ( this.WriteThreshold != null && end >= this.WriteThreshold ) )
            {
                return false;
            }

            registerReadLockAcquired( tx, tle, start, end );
            return true;
        }
        finally
        {
            // if deadlocked, remove marking so lock is removed when empty
            tle.movedOn = true;
            marked--;
        }
    }

    synchronized boolean tryAcquireWriteLock( LockTransaction tx, int time )
    {
        TxLockElement tle = getOrCreateLockElement( tx );

        try
        {
            tle.movedOn = false;
            if ( tle.writeThreadshold == null && ( (  this.WriteThreshold != null ) || ( this.readIntervalList.union( new TimeInterval( time, time ) ) ) ) )
            {
                return false;
            }

            registerWriteLockAcquired( tx, tle, time );
            return true;
        }
        finally
        {
            // if deadlocked, remove marking so lock is removed when empty
            tle.movedOn = true;
            marked--;
        }
    }

    synchronized void acquireWriteLock( LockTransaction tx, int time )
    {
        TxLockElement tle = getOrCreateLockElement( tx );

        try
        {
            tle.movedOn = false;

            boolean shouldAddWait = true;
            Thread currentThread = currentThread();

            while ( tle.writeThreadshold == null && ( (  this.WriteThreshold != null ) || ( this.readIntervalList.union( new TimeInterval( time, time ) ) ) ) )
            {
                ragManager.checkWaitOn( this, tx );

                if (shouldAddWait)
                {
                    waitingThreadList.addFirst( new WaitElement( tle, WRITE, currentThread) );
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

            registerWriteLockAcquired( tx, tle, time );
        }
        finally
        {
            // if deadlocked, remove marking so lock is removed when empty
            tle.movedOn = true;
            marked--;
        }
    }

    synchronized void acquireReadLock( LockTransaction tx, int start, int end )
    {
        TxLockElement tle = getOrCreateLockElement( tx );

        try
        {
            tle.movedOn = false;

            boolean shouldAddWait = true;
            Thread currentThread = currentThread();

            while ( tle.writeThreadshold == null && ( this.WriteThreshold != null && end >= this.WriteThreshold ) )
            {
                ragManager.checkWaitOn( this, tx );

                if (shouldAddWait)
                {
                    waitingThreadList.addFirst( new WaitElement( tle, READ, currentThread) );
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
            
            registerReadLockAcquired( tx, tle, start, end );
        }
        finally
        {
            // if deadlocked, remove marking so lock is removed when empty
            tle.movedOn = true;
            marked--;
        }
    }
    
    private Integer WriteThreshold = null;
    private TimeIntervalList readIntervalList = null;
    
    private final Object resource; // the resource this RWLock locks
    private final LinkedList<WaitElement> waitingThreadList = new LinkedList<>();
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
        private int readCount;
        private int writeCount;
        private Integer writeThreadshold = null;
        private boolean movedOn;

        TxLockElement( Object tx )
        {
            this.tx = tx;
        }

        boolean isFree()
        {
            return readCount == 0 && writeThreadshold == null;
        }
    }

    // keeps track of what type of lock a thread is waiting for
    private static class WaitElement
    {
        private final TxLockElement element;
        private final LockType lockType;
        private final Thread waitingThread;
        private final long since = System.currentTimeMillis();

        WaitElement( TxLockElement element, LockType lockType, Thread thread )
        {
            this.element = element;
            this.lockType = lockType;
            this.waitingThread = thread;
        }
    }

    public Object resource()
    {
        return resource;
    }

    synchronized void mark()
    {
        this.marked++;
    }

    synchronized boolean isMarked()
    {
        return marked > 0;
    }

    /**
     * Tries to acquire read lock for a given transaction. If
     * <CODE>this.writeCount</CODE> is greater than the currents tx's write
     * count the transaction has to wait and the {@link RagManager#checkWaitOn}
     * method is invoked for deadlock detection.
     * <p>
     * If the lock can be acquired the lock count is updated on <CODE>this</CODE>
     * and the transaction lock element (tle).
     *
     * @throws DeadlockDetectedException
     *             if a deadlock is detected
     */
    synchronized void acquireReadLock( Object tx ) throws DeadlockDetectedException
    {
        // do nothing
    }

    synchronized boolean tryAcquireReadLock( Object tx )
    {
        // do nothing
        return false;
    }

    /**
     * Releases the read lock held by the provided transaction. If it is null then
     * an attempt to acquire the current transaction will be made. This is to
     * make safe calling the method from the context of an
     * <code>afterCompletion()</code> hook where the tx is locally stored and
     * not necessarily available through the tm. If there are waiting
     * transactions in the queue they will be interrupted if they can acquire
     * the lock.
     */
    synchronized void releaseReadLock( Object tx ) throws LockNotFoundException
    {
        // do nothing
    }

    /**
     * Calls {@link #acquireWriteLock(Object)} with the
     * transaction associated with the current thread.
     * @throws DeadlockDetectedException
     */
    void acquireWriteLock() throws DeadlockDetectedException
    {
        acquireWriteLock( null );
    }

    /**
     * Tries to acquire write lock for a given transaction. If
     * <CODE>this.writeCount</CODE> is greater than the currents tx's write
     * count or the read count is greater than the currents tx's read count the
     * transaction has to wait and the {@link RagManager#checkWaitOn} method is
     * invoked for deadlock detection.
     * <p>
     * If the lock can be acquires the lock count is updated on <CODE>this</CODE>
     * and the transaction lock element (tle).
     *
     * @throws DeadlockDetectedException
     *             if a deadlock is detected
     */
    synchronized void acquireWriteLock( Object tx ) throws DeadlockDetectedException
    {
        // do nothing
    }

    synchronized boolean tryAcquireWriteLock( Object tx )
    {
        // do nothing
        return false;
    }

    /**
     * Releases the write lock held by the provided tx. If it is null then an
     * attempt to acquire the current transaction from the transaction manager
     * will be made. This is to make safe calling this method as an
     * <code>afterCompletion()</code> hook where the transaction context is not
     * necessarily available. If write count is zero and there are waiting
     * transactions in the queue they will be interrupted if they can acquire
     * the lock.
     */
    synchronized void releaseWriteLock( Object tx ) throws LockNotFoundException
    {
        // do nothing
    }

    int getWriteCount()
    {
        return totalWriteCount;
    }

    int getReadCount()
    {
        return totalReadCount;
    }

    synchronized int getWaitingThreadsCount()
    {
        return waitingThreadList.size();
    }

    @Override
    public synchronized boolean visit( LineLogger logger )
    {
        logger.logLine( "Total lock count: readCount=" + totalReadCount
            + " writeCount=" + totalWriteCount + " for " + resource );

        logger.logLine( "Waiting list:" );
        Iterator<WaitElement> wElements = waitingThreadList.iterator();
        while ( wElements.hasNext() )
        {
            WaitElement we = wElements.next();
            logger.logLine( "[" + we.waitingThread + "("
                + we.element.readCount + "r," + we.element.writeCount + "w),"
                + we.lockType + "]" );
            if ( wElements.hasNext() )
            {
                logger.logLine( "," );
            }
            else
            {
                logger.logLine( "" );
            }
        }

        logger.logLine( "Locking transactions:" );
        Iterator<TxLockElement> lElements = txLockElementMap.values().iterator();
        while ( lElements.hasNext() )
        {
            TxLockElement tle = lElements.next();
            logger.logLine( "" + tle.tx + "(" + tle.readCount + "r,"
                + tle.writeCount + "w)" );
        }
        return true;
    }

    public synchronized String describe()
    {
        StringBuilder sb = new StringBuilder( this.toString() );
        sb.append( " Total lock count: readCount=" + totalReadCount
                + " writeCount=" + totalWriteCount + " for " + resource + "\n" )
          .append( "Waiting list:" + "\n" );
        Iterator<WaitElement> wElements = waitingThreadList.iterator();
        while ( wElements.hasNext() )
        {
            WaitElement we = wElements.next();
            sb.append( "[" + we.waitingThread + "("
                    + we.element.readCount + "r," + we.element.writeCount + "w),"
                    + we.lockType + "]\n" );
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
            sb.append( "" + tle.tx + "(" + tle.readCount + "r,"
                    + tle.writeCount + "w)\n" );
        }
        return sb.toString();
    }

    public synchronized long maxWaitTime()
    {
        long max = 0l;
        for ( WaitElement thread : waitingThreadList )
        {
            if ( thread.since < max )
            {
                max = thread.since;
            }
        }
        return System.currentTimeMillis() - max;
    }

    @Override
    public String toString()
    {
        return "RWLock[" + resource + ", hash="+hashCode()+"]";
    }

    private void registerReadLockAcquired( Object tx, TxLockElement tle, int start, int end )
    {
        registerLockAcquired( tx, tle );
//        totalReadCount++;
        tle.readCount++;
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

    synchronized Object getTxLockElementCount()
    {
        return txLockElementMap.size();
    }
}
