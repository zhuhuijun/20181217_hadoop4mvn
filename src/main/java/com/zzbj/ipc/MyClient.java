package com.zzbj.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {
	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		HelloWorldService proxy;
		try {
			proxy = RPC.getProxy(HelloWorldService.class//
					, HelloWorldService.versionID
					, new InetSocketAddress("localhost", 8888), //
					configuration);
			String result = proxy.sayHello("world");
			System.out.println("client console:" + result);
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("over!!!!!!!!!!!");
	}
}
