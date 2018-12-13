package core;

import java.util.LinkedHashMap;
import java.util.Map;

public class MRUMap<K,V> extends LinkedHashMap<K,V>
{
  private static final long serialVersionUID = 1L;

  /** Value most recently removed from map */
  Object removedValue;
  
  /** Maximum number of entries allowed in this map */
  private int maxEntries;

  /**
   * Constructor
   *  
   * @param maxEntries
   *            Maximum number of entries allowed in the map
   */
  public MRUMap(final int maxEntries)
  {
    super(10, 0.75f, true);

    if (maxEntries <= 0)
    {
      throw new IllegalArgumentException("Must have at least one entry");
    }
    
    this.maxEntries = maxEntries;
  }
  
  /**
   * @return Returns the removedValue.
   */
  public Object getRemovedValue()
  {
    return removedValue;
  }
  
  /**
   * @see java.util.LinkedHashMap#removeEldestEntry(java.util.Map.Entry)
   */
  protected boolean removeEldestEntry(final Map.Entry<K,V> eldest)
  {
    final boolean remove = size() > maxEntries;
    // when it should be removed remember the oldest value that will be removed
    if (remove)
    {
      this.removedValue = eldest.getValue();
    }
    else
    {
      removedValue = null;
    }
    return remove;
  }
}