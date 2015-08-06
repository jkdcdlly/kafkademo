package org.apache.kafka.kafkademo;

public class Test extends Thread {
	private int i = 0;

	public Test(int i) {
		this.i = i;
	}

	@Override
	public void run() {
		System.out.println(++i);
	}

	public static void main(String[] args) {
		Test t1 = new Test(0);
		t1.start();
		Test t2 = new Test(0);
		t2.start();
	}
}
