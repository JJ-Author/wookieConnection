package org.bio_gene.wookie.exceptions;

public class NotSupportedException extends Exception {

	private static String MESSAGE="This Feature is currently not supported";
	
	public NotSupportedException() {
        super(MESSAGE);
    }
}
