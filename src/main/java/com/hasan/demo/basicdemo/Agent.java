package com.hasan.demo.basicdemo;

public class Agent extends Thread {

	int tag;
	int data;

	public Agent(int tag, int data) {

		this.tag = tag;
		this.data = data;
	}

	public int getTag() {
		return tag;
	}

	public void setId(int id) {
		this.tag = id;
	}

	public int getData() {
		return data;
	}

	public void setData(int data) {
		this.data = data;
	}

}
