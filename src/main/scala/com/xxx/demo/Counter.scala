package com.xxx.demo

class Counter {
  private var value = 0
  def increment(step : Int):Unit = {value += step}

  def current():Int = {
    value
    value + 1
  }

  def value2 = value
  def value2_= (newValue:Int): Unit ={
    value = newValue
  }
}

object Test{
  def main(args: Array[String]): Unit = {
    val counter = new Counter()

    counter.increment(2)
    println(counter.current())

    counter.value2_=(5)

    println(counter.value2)
  }
}
