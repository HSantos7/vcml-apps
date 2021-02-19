package voldemort.consistency.utils;

public class BitsUtils {

	
	static long getPhysicalTime (){
		return System.currentTimeMillis();
	}

	public static long shiftToHighBits(long time) {
		return time << 16;
	}
	
	public static long getHigherBits(long time) {
		return time & 0xFFFFFFFFFFFF0000L;
	}
	
	public static long getLowerBits(long time) {
		return time & 0x000000000000FFFFL;
	}
}
