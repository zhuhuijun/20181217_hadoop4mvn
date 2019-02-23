package com.zzbj.ipc;

import org.apache.hadoop.ipc.VersionedProtocol;
/***
 * 定义接口
 * @author zhuhuijun
 *
 */
public interface HelloWorldService extends VersionedProtocol {
	public static final long versionID = 1;
	public String sayHello(String msg);
}
