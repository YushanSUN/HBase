import java.io.IOException;

import junit.framework.TestCase;

public abstract class BaseTests extends TestCase {
	protected InvertedIndex index;

	public BaseTests() {
		super();

		try {
			index = new InvertedIndex();
		} catch (IOException e) {
			System.err.println("Cannot load index");
		}
	}

	public void testIndexLoaded() {
		assertNotNull(index);
	}
}