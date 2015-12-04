import java.io.IOException;

/**
 * This class contains the public test cases for the project. The public tests
 * are distributed with the project description, and students can run the public
 * tests themselves against their own implementation.
 * 
 * Any changes to this file will be ignored when testing your project.
 * 
 */
public class PublicTests extends BaseTests {
	public void testNoStopWord() {
		try {
			assertEquals(0,index.getNumberColumns("to"));
		} catch(IOException e) {
			fail(e.getMessage());
		}
	}
	
	public void testNoNumber() {
		try {
			assertEquals(0,index.getNumberColumns("2000"));
		} catch(IOException e) {
			fail(e.getMessage());
		}
	}
	
	public void testTermsAreStemmed() {
		try {
			assertEquals(0,index.getNumberColumns("university"));
		} catch(IOException e) {
			fail(e.getMessage());
		}
	}
	
	public void testNoNamespace() {
		try {
			assertTrue(index.getScore("decad", "Category:2000")==-1);
		} catch(IOException e) {
			fail(e.getMessage());
		}
	}
	
	public void testEntryExists() {
		try {
			assertTrue(index.getScore("franc", "François Hollande")>0);
		} catch(IOException e) {
			fail(e.getMessage());
		}
	}
	
	public void testReadableScore() {
		try {
			assertTrue(index.getScore("presid", "Nicolas Sarkozy")>0);
		} catch(IOException e) {
			fail(e.getMessage());
		}
	}	

	public void testSizePostingList() {
		try {
			assertEquals(349,index.getNumberColumns("rabbit"));
		} catch(IOException e) {
			fail(e.getMessage());
		}
	}
	
	public void testCorrectScore() {
		try {
			assertEquals(0.05142, index.getScore("presid", "Barack Obama"), 0.0001);
		} catch(IOException e) {
			fail(e.getMessage());
		}
	}
}
