package com.zzbj.hadoop.sort.secondary;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class UtilHelper {
	/***
	 * 获得组名
	 * 
	 * @param name
	 * @param hash
	 * @return
	 */
	public static String GetGrp(String name, int hash) {
		String hostName;
		try {
			hostName = InetAddress.getLocalHost().getHostName();
			long time = System.nanoTime();
			return "[" + hostName + "]." + hash + "." + time + ":" + name;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	/***
	 * 
	 * <p>
	 * 获得组名dddddddddddddddd
	 * <p>
	 * 
	 * @param name
	 * @param hash
	 * @return
	 */
	public static String GetGrp2(String name, int hash) {
		String hostName;
		try {
			hostName = InetAddress.getLocalHost().getHostName();
			long time = System.nanoTime();
			return "[" + hostName + "]." + hash + ":" + name;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

}
