package com.skyshv.Buffer;

import com.skyshv.utils.IInstGen;
import com.skyshv.utils.ITransfer;
import com.skyshv.utils.UnSafeWrapper;

import java.lang.reflect.Array;
import java.util.stream.IntStream;


public class BlockBuffer<T extends ITransfer<T> > {
    private volatile T[][] buffer;
    private Class<T> clazz;
    private IInstGen<T> instGen;

    static long bufferOffset;

    private static final int BLOCKBITS = 16;
    private static final int BLOCKSIZES = 1 << BLOCKBITS;
    private static final int BLOCKMASKS = BLOCKSIZES - 1;


    static {
        try {
            bufferOffset = UnSafeWrapper.unSafe.objectFieldOffset(BlockBuffer.class.getDeclaredField("buffer"));
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    public BlockBuffer(Class<T> c, IInstGen<T> igen) {
        this.clazz = c;
        this.instGen = igen;
        buffer = (T[][]) Array.newInstance(c, 1, BLOCKSIZES);
        buffer[0] = newBlock();

    }

    private void checkRange(long rowidx) {
        final long l = rowidx >> BLOCKBITS;
        if( l > Integer.MAX_VALUE || rowidx < 0 ){
            throw new RuntimeException("BlockBuffer range overflow " + rowidx);
        }
    }

    public void ensureCapacity(long rowidx ){
        checkRange(rowidx);
        final long l = rowidx >> BLOCKBITS;
        T[][] newBuff = null, oldBuff = null;
        do {
            oldBuff = buffer;
            if( l < oldBuff.length ){
                return;
            }else{
                newBuff = (T[][]) Array.newInstance(clazz, (int) Math.max(l+1, oldBuff.length << 1 ), BLOCKSIZES);
                System.arraycopy(oldBuff, 0, newBuff, 0, oldBuff.length);
                for( int i = oldBuff.length; i < newBuff.length; i++ ){
                    newBuff[i] = newBlock();
                }
            }
        }while(!UnSafeWrapper.unSafe.compareAndSwapObject(this, bufferOffset, oldBuff, newBuff));
    }

    public void set(long rowidx, T value ){
        ensureCapacity(rowidx);
        T old = buffer[(int) (rowidx >> BLOCKBITS)][(int) (rowidx & BLOCKMASKS)];
        if( value.Overwritable(old)) {
            buffer[(int) (rowidx >> BLOCKBITS)][(int) (rowidx & BLOCKMASKS)] = value;
        }
    }

    public T get(long rowidx) {
        checkRange(rowidx);
        return buffer[(int) (rowidx >> BLOCKBITS)][(int) (rowidx& BLOCKMASKS)];
    }

    private T[] newBlock() {
        final T[] ts = (T[]) Array.newInstance(clazz, BLOCKSIZES);
        if( this.instGen != null ){
            IntStream.range(0, BLOCKSIZES).parallel().forEach( e -> {
                ts[e] = this.instGen.newInst();
            });
        }
        return ts;
    }
}
