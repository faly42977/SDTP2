package test.misc;

import java.util.ArrayList;

import utils.ServiceDiscoveryKafka;

public class testKafka {
	
	public static void main (String[] args) {
		ServiceDiscoveryKafka kafka = new ServiceDiscoveryKafka();
		kafka.write("Datanode", "d1");
		kafka.write("Datanode", "d2");
		kafka.write("Datanode", "d3");
		kafka.write("Datanode", "d4");
		
		kafka.write("Namenode", "n1");
		kafka.write("Namenode", "n2");
		kafka.write("Namenode", "n3");
		kafka.write("Namenode", "n4");
		
		ArrayList<String> list  = kafka.listen("Datanode");
		for (String s : list)
			System.out.println(s);
		
		
		 list  = kafka.listen("Namenode");
		for (String s : list)
			System.out.println(s);
		
	}

}
