package bufmgr;

import diskmgr.*;
import global.*;

public class BufDesc {
	
	private PageId pageNumber = null;
	public int pinCount;
	private boolean dirtyBit;
	private boolean loved;
	
	public BufDesc(){
		
		pageNumber = new PageId();
		pinCount = 0;
		dirtyBit = false;
		// Hated By Default:
		loved = false;
	}//end cons.
	
	public boolean isDirty(){
		return dirtyBit;
	}
	
	public int getPageNumber(){
		return pageNumber.pid;
	}
	
	public int getPinCount(){
		return pinCount;
	}
	
	public void incrementPinCount(){
		pinCount++;
	}
	
	public void decrementPinCount(){
		pinCount--;
	}
	
	public void setPinCount(int num){
		pinCount = num;
	}
	
	public void setPageNumber(int num){
		pageNumber.pid = num;
	}
	
	public void setDirtyBit(boolean b){
		dirtyBit = b;
	}
	
	public void setLoved( boolean b ){
		loved = b;
	}
	
	public boolean getLoved(){
		return loved;
	}
	
}//end class.
