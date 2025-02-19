package org.apache.hadoop.hbase.trace;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class WrapContext <T> {
  public static int id = 0;

  public T value;

  public T getValue(){
    System.out.println("FL, getValue() called with id: " + id + " value: " + value);
    id++;
    return value;
  }

  public WrapContext(T value) {
    System.out.println("FL, WrapContext constructor called with value: " + value + " id: " + id++);
    this.value = value;
  }
}
