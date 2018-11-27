package core;

public class Symbol implements Comparable<Symbol>
{
    private final String t;
    private final int hashCode;

    public Symbol(String t)
    {
        this.t = t;
        this.hashCode = t.hashCode();
    }
    
    public Symbol(char[] t)
    {
    	this(new String(t));
    }

    public final String get()
    {
        return t;
    }
    
    public final char[] asArray()
    {
    	return t.toCharArray();
    }

    @Override
    public int hashCode()
    {
        return this.hashCode;
    }

    @Override
    public final boolean equals(Object that)
    {
        return ((that instanceof Symbol) ? 
                           (this.t.equals(((Symbol)that).t)) : false);
    }
    
    @Override
    public final String toString()
    {
    	return this.t;
    }
    
    @Override
    public final int compareTo(Symbol other)
    {
    	return this.t.compareTo(other.toString());
    }
}