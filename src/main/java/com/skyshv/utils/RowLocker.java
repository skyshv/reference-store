package com.skyshv.utils;

import com.skyshv.Buffer.BlockBuffer;

public class RowLocker {

    BlockBuffer<LockerEntry> rowBuffers = new BlockBuffer<>(LockerEntry.class, new IInstGen<LockerEntry>() {
        @Override
        public LockerEntry newInst() {
            return new LockerEntry();
        }
    });

    public void aquire(long rowidx) {
        rowBuffers.ensureCapacity(rowidx);
        LockerEntry lockerEntry = rowBuffers.get(rowidx);
        while (!UnSafeWrapper.unSafe.compareAndSwapObject(lockerEntry, LockerEntry.otOffset, null, Thread.currentThread()))
            ;
        return;
    }

    public void release(long rowidx){
        LockerEntry lockerEntry = rowBuffers.get(rowidx);
        if( lockerEntry.ot != Thread.currentThread() ){
            throw new RuntimeException("Try to release non owner lock " + rowidx);
        }
        lockerEntry.ot = null;
    }

    static public class LockerEntry implements ITransfer<LockerEntry>{
        volatile Thread ot;
        static long otOffset;

        static {
            try {
                otOffset = UnSafeWrapper.unSafe.objectFieldOffset(LockerEntry.class.getDeclaredField("ot"));
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
        }
        @Override
        final public boolean Overwritable(LockerEntry oldv) {
            return oldv == null;
        }
    }
}
