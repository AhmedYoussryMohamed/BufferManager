/* File Page.java */

package diskmgr;

import global.*;

 /**
  * class Page
  */
	
public class Page implements GlobalConst{
  
  
  /**
   * default constructor
   */
 
  public Page()  
    {
      data = new byte[MAX_SPACE];
      
    }
  
  /**
   * Constructor of class Page
   */
  public Page(byte [] apage)
    {
      data = apage;
    }
  
  /**
   * return the data byte array
   * @return 	the byte array of the page
   */
  public byte [] getpage()
    {
      return data;
      
    }
  
  /**
   * set the page with the given byte array
   * @param 	array   a byte array of page size
   */
  public void setpage(byte [] array)
    {
      data = array;
    }
  
  /**
   * private field: An array of bytes 
   * 
   */
  protected byte [] data;
  
  int id = 0;
  public void setId(int x){
	  id = x;
  }
  public int getId(){
	  return id;
  }
  
  public int getAdd(){
	  return data.hashCode();
  }
  
}//end class.
