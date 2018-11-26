package core;

public class KVPair {

    public char[] element1;
    public char[] element2;

    public KVPair(char[] element1, char[] element2) {
	this.element1 = element1;
	this.element2 = element2;
    }
    
    public KVPair(String elem1, String elem2)
    {
    	this.element1 = elem1.toCharArray();
    	this.element2 = elem2.toCharArray();
    }
}
