package com.lsp.set;

import java.util.HashSet;
import java.util.Set;

import com.lsp.model.Person;

public class HashSetDemo {

	public static void main(String[] args) {
		Set<Person> hs = new HashSet<Person>();
		Person p1 = new Person("William", 32);
		Person p2 = new Person("Jerry", 22);
		Person p3 = new Person("Jackey", 30);
		Person p4 = new Person("William", 32);
		hs.add(p1);
		hs.add(p2);
		hs.add(p3);
		hs.add(p4);
		
//		System.out.println(hs);
//		
//		System.out.println(p1 == p2);
////		System.out.println(p1.equals(p2));
//		System.out.println(p1 == p4);
////		System.out.println(p1.equals(p4));
	}

}
