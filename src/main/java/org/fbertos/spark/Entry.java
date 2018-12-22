package org.fbertos.spark;

public class Entry {
	private String ip;
	private String date;
	
	public Entry(String ip, String date) {
		this.ip = ip;
		this.date = date;
	}
	
	public String toString() {
		return this.ip + "|" + this.date;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
}