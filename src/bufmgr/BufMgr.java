package bufmgr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;


import chainexception.ChainException;

import diskmgr.*;
import tests.*;
import global.*;

public class BufMgr {

	private byte[][] bufPool;
	private BufDesc[] bufDesc;
	private String replacementPolicy;
	private int numBufs;
	private int countFrames = 0;
	private HashMap<Integer, Integer> bufPoolMap;
	private Queue<Integer> fifoQueue = null;
	private Queue<Integer> lruQueue = null;
	private Stack<Integer> mruStack = null;
	
	public BufMgr(int numBufs, String replaceArg) {
		bufPool = new byte[numBufs][GlobalConst.MINIBASE_PAGESIZE];
		bufDesc = new BufDesc[numBufs];
		replacementPolicy = replaceArg;
		bufPoolMap = new HashMap<Integer, Integer>();
		this.numBufs = numBufs;

		for (int i = 0; i < numBufs; i++) {
			bufDesc[i] = new BufDesc();
		}// end for i.

		// talfe2:
		if (replaceArg.equalsIgnoreCase("CLOCK")) {
			replacementPolicy = "FIFO";
		}

		if (replacementPolicy.equalsIgnoreCase("FIFO")) {
			fifoQueue = new LinkedList<Integer>();
			
		} 
		else if (replacementPolicy.equalsIgnoreCase("LRU")) {
			lruQueue = new LinkedList<Integer>();

		} else if( replacementPolicy.equalsIgnoreCase("MRU") ){
			mruStack = new Stack<Integer>();

		} else{
			replacementPolicy = "Love/Hate";
			lruQueue = new LinkedList<Integer>();
			mruStack = new Stack<Integer>();
		}//end else.
		
	}// end Constructor.

	public void pinPage(PageId pgid, Page page, boolean emptyPage, boolean loved) throws ChainException {
//		System.out.println( bufPoolMap.size() +" " + pgid.pid);
		
		if (bufPoolMap.containsKey(pgid.pid)) {
			
			int frameNum =  bufPoolMap.get(pgid.pid);
			bufDesc[ frameNum ].incrementPinCount();

			page.setpage( bufPool[frameNum] );
			
			if( replacementPolicy.equalsIgnoreCase("FIFO") && fifoQueue.contains(pgid.pid) ){
				fifoQueue.remove(pgid.pid);
			}
		} else {
			
			if (countFrames < numBufs) {
				bufPoolMap.put(pgid.pid, countFrames);
				
				page.setpage( bufPool[countFrames] );
				readPage( pgid , page );
				
				bufDesc[countFrames].setPageNumber( pgid.pid );
				bufDesc[countFrames].incrementPinCount();
				
				if (replacementPolicy.equalsIgnoreCase("FIFO")) {
//					fifoQ.poll();
				} else if (replacementPolicy.equalsIgnoreCase("LRU")) {
					lruQueue.add( pgid.pid );
				} else if( replacementPolicy.equalsIgnoreCase("MRU") ){
					mruStack.push( pgid.pid );
				}else{
					//LOVE / HATE:
					lruQueue.add(pgid.pid);
					mruStack.push( pgid.pid );
				}
				countFrames++;

			} else {
				if (replacementPolicy.equalsIgnoreCase("FIFO")) {//-----------------------------------------------------------------
					
					boolean foundFreeFrame = fifoPolicy(pgid, page);
					if ( !foundFreeFrame ) {
						System.out.println("----------------------NO replacement candidates in FIFO.");
						 throw new BufferPoolExceededException(new Exception(), "NO replacement candidates in FIFO.");
					}
					
				} else if (replacementPolicy.equalsIgnoreCase("LRU")) { //-------------------------------------------------------------------------
					
					boolean foundFreeFrame = lruPolicy(pgid, page);
					
					if( !foundFreeFrame ){
						System.out.println("----------------------NO replacement candidates in LRU.");
						throw new BufferPoolExceededException(new Exception(), "NO replacement candidates in LRU.");
					}
					
				} else if( replacementPolicy.equalsIgnoreCase("MRU") ){//--------------------------------------------------------------------------
					
					boolean foundFreeFrame = mruPolicy( pgid ,page );
					
					if( !foundFreeFrame ){
						System.out.println("----------------------NO replacement candidates in MRU.");
						throw new BufferPoolExceededException(new Exception(), "NO replacement candidates in MRU.");
					}

				} else{ //-------------------------------------------------------------------------------------------------------------------------------
					//LOVE / HATE.
					boolean foundFreeFrame = lruPolicy(pgid, page);
					
					if( !foundFreeFrame ){
						foundFreeFrame = mruPolicy(pgid ,page);
					}
					
					if( !foundFreeFrame ){
						System.out.println("----------------------NO replacement candidates in Love/Hate.");
						throw new BufferPoolExceededException(new Exception(), "NO replacement candidates in Love/Hate.");
					}
					
				}// end else replacement.
			}// end else countFrames.
		}// end else map.
		
	}// end method.

	public void unpinPage(PageId pgid, boolean dirty, boolean loved)
			throws PageUnpinnedException, HashEntryNotFoundException {

		if (!bufPoolMap.containsKey(pgid.pid)) {
			System.out.println("----------------------bufPoolMap doesnot Contain key in unpinPage()");
			throw new HashEntryNotFoundException(new Exception(), "Unpin Page Error");
		}

		int frameNum = bufPoolMap.get(pgid.pid);
		
		if( dirty ){
			bufDesc[frameNum].setDirtyBit(dirty);
		}

		if (bufDesc[frameNum].getPinCount() == 0) {
			System.out.println("----------------------PinCount = 0 in unPinPage()");
			throw new PageUnpinnedException(new Exception(), "Unpin Page Error");
		} else {
			bufDesc[frameNum].decrementPinCount();
			
			bufPoolMap.put(pgid.pid, frameNum);
			
			if( !bufDesc[frameNum].getLoved() && loved && replacementPolicy.equalsIgnoreCase("Love/Hate")){
				bufDesc[frameNum].setLoved(loved);
				
				if( lruQueue.contains( pgid.pid ) ){
					lruQueue.remove( pgid.pid );
					//Already Pushed:
//					mruStack.push( pgid.pid );
				}
				
			}//end if.
			
			
			if (bufDesc[frameNum].getPinCount() == 0) {
				if (replacementPolicy.equalsIgnoreCase("FIFO") ) {
					fifoQueue.add(pgid.pid);
				}
			}// end if.
			
		}// end else pincount = 0
		
	}// end method.

	public PageId newPage(Page firstPage, int howmany) throws ChainException {

		if (!isBufferFull()) {
			PageId firstPageId = new PageId();
			try {
				(SystemDefs.JavabaseDB).allocate_page(firstPageId, howmany);
				pinPage(firstPageId, firstPage, false, true);
			} catch (OutOfSpaceException | InvalidRunSizeException
					| InvalidPageNumberException | FileIOException
					| DiskMgrException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}// end catch

			return firstPageId;

		} else {
			// Buffer Full:
			PageId firstPageId = new PageId();
			try {
				(SystemDefs.JavabaseDB).deallocate_page(firstPageId, howmany);
			} catch (InvalidRunSizeException | InvalidPageNumberException
					| FileIOException | DiskMgrException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}// end else.

	}// end method.

	public void freePage(PageId pgid) throws PagePinnedException, PageUnpinnedException, HashEntryNotFoundException {
		
		if( pgid != null && bufPoolMap.containsKey(pgid.pid) ){
			int frameNum = bufPoolMap.get( pgid.pid );
			if( bufDesc[frameNum].getPinCount() > 1 ){
				throw new PagePinnedException(new Exception(), "In Free Page Error");
			}
			if (bufDesc[frameNum].getPinCount()==1) {
				unpinPage( pgid ,bufDesc[frameNum].isDirty(),false);
			}
			
		}
		try {
			(SystemDefs.JavabaseDB).deallocate_page(pgid);
		} catch (InvalidRunSizeException | InvalidPageNumberException
				| FileIOException | DiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// end catch

	}// end method.

	public void flushPage(PageId pgid) {

		int frameNum = bufPoolMap.get( pgid.pid );
		
		Page page = new Page(bufPool[frameNum]);
		bufDesc[frameNum].setDirtyBit(false);
		try {
			(SystemDefs.JavabaseDB).write_page(pgid, page);
		} catch (InvalidPageNumberException | FileIOException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}// end method.

	public int getNumUnpinnedBuffers() {
		int numUnpinnedBuffers = 0;
		for (int i = 0; i < bufDesc.length; i++) {
			if (bufDesc[i].getPinCount() == 0) {
				numUnpinnedBuffers++;
			}
		}// end for.
		return numUnpinnedBuffers;
	}

	public boolean isBufferFull() {

		for (int i = 0; i < numBufs; i++) {
			if (bufDesc[i].getPinCount() == 0) {
				return false;
			}
		}

		return true;
	}// end method.

	public void flushAllPages() {

		for (int id : bufPoolMap.keySet()) {
			PageId pgid = new  PageId();
			pgid.pid = id;
			flushPage(pgid);
		}

	}// end method.
	
	public void readPage(PageId pgid ,Page page){
		
		try {
			(SystemDefs.JavabaseDB).read_page(pgid, page);
		} catch (InvalidPageNumberException | FileIOException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}//end method.
	
	public boolean lruPolicy( PageId pgid ,Page page ){
		
		Queue<Integer> tempQueue = new LinkedList<Integer>();
		boolean foundFreeFrame = false;
		
		while( !lruQueue.isEmpty() ){
			int key = lruQueue.poll();
			int frameNum = bufPoolMap.get( key );
			boolean operate = true;
			if( bufDesc[frameNum].getPinCount() > 0 ){
				tempQueue.add( key );
			}else if( bufDesc[frameNum].getPinCount() == 0 ){
				if( foundFreeFrame ){
					tempQueue.add( key );
				}else{
					if( replacementPolicy.equalsIgnoreCase("Love/Hate") && bufDesc[frameNum].getLoved() ){
						operate = false;
					}
					if( operate ){
						foundFreeFrame = true;
						if (bufDesc[frameNum].isDirty()) {
							PageId check = new PageId();
							check.pid = bufDesc[frameNum].getPageNumber();
							flushPage( check );
						}
						page.setpage( bufPool[frameNum] );
						readPage( pgid , page );
						
						bufPoolMap.put(pgid.pid, frameNum);

						bufDesc[frameNum].setDirtyBit(false);
						bufDesc[frameNum].setPageNumber(pgid.pid);
						bufDesc[frameNum].incrementPinCount();
						
						if( bufPoolMap.containsKey(key) ){ 
							bufPoolMap.remove(key);
						}

					}//end operate.
				}//end else foundFree.
				
			}//end else.
			
		}//end while empty.
		
		while( !tempQueue.isEmpty() ){
			lruQueue.add( tempQueue.poll() );
		}//end while.
		
		if( foundFreeFrame ){
			lruQueue.add( pgid.pid );
		}
		
		return foundFreeFrame;
	}//end method.
	
	public boolean mruPolicy( PageId pgid ,Page page ){
		
		Stack<Integer> tempStack = new Stack<Integer>();
		boolean foundFreeFrame = false;
		
		while( !mruStack.isEmpty() ){
			int key = mruStack.pop();
			int frameNum = bufPoolMap.get( key );
			boolean operate = true;
			
			if( bufDesc[frameNum].getPinCount() > 0 ){
				tempStack.push( key );
			}else if( bufDesc[frameNum].getPinCount() == 0 ){
				if( foundFreeFrame ){
					tempStack.add( key );
				}else{
					if( replacementPolicy.equalsIgnoreCase("Love/Hate") && !bufDesc[frameNum].getLoved() ){
						operate = false;
					}
					if( operate ){
						foundFreeFrame = true;
						if (bufDesc[frameNum].isDirty()) {
							PageId check = new PageId();
							check.pid = bufDesc[frameNum].getPageNumber();
							flushPage( check );
						}
						page.setpage( bufPool[frameNum] );
						readPage( pgid , page );
						
						bufPoolMap.put(pgid.pid, frameNum);
						
						bufDesc[frameNum].setDirtyBit(false);
						bufDesc[frameNum].setPageNumber(pgid.pid);
						bufDesc[frameNum].incrementPinCount();
						
						if( bufPoolMap.containsKey(key) ){ 
							bufPoolMap.remove(key);
						}
					}//end operate.
					
				}//end else foundFree.
				
			}//end else.
			
		}//end while empty.
		
		
		
		while( !tempStack.isEmpty() ){
			mruStack.push( tempStack.pop() );
		}//end while.
		
		if( foundFreeFrame ){
			mruStack.push( pgid.pid );
		}
		
		return foundFreeFrame;
		
	}//end method.
	
	public boolean fifoPolicy( PageId pgid ,Page page ){
		if (fifoQueue.size() == 0) {
			return false;
		} else {
			
			int key = fifoQueue.poll();
			int frameNum = bufPoolMap.get( key );
			
			if ( bufDesc[frameNum].isDirty() ) {
				PageId check = new PageId();
				check.pid = bufDesc[frameNum].getPageNumber();
				flushPage( check );
			}
			
			page.setpage( bufPool[frameNum] );
			readPage( pgid , page );
			
            bufPoolMap.put(pgid.pid, frameNum);
			
			bufDesc[frameNum].setDirtyBit(false);
			bufDesc[frameNum].setPageNumber(pgid.pid);
			bufDesc[frameNum].incrementPinCount();
			
			if( bufPoolMap.containsKey(key) ){
				bufPoolMap.remove(key);
			}
			return true;
		}// end else queueSize.
		
	}//end method.
	
	public void check(){
		System.out.println("MAP CHECK : _-----------------------------------------_");
		for( int id : bufPoolMap.keySet() ){
			System.out.println("   Id : " + id + " Frame " + bufPoolMap.get(id) );
		}
		
	}//end method.
	
	public void check2(){
		
		System.out.println("new------------------------------------------------------------------ ");
		for( int i = 0 ;i < bufPool[0].length ;i++){
			System.out.print( bufPool[0][i] + "    ");
		}
		System.out.println();
	}
	
}// end class.
