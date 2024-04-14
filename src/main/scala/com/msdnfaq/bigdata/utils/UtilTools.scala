package com.msdnfaq.bigdata.utils

import java.io.UnsupportedEncodingException
import scala.util.Random

object UtilTools {
  def getChinese: String = {
    var nameCN: String = null
    var highPos = 0
    var lowPos = 0 //区码，0xA0打头，从第16区开始，即0xB0=11*16=176,16~55一级汉字，56~87二级汉字
    highPos = 176 + Math.abs(Random.nextInt(71)) //位码，0xA0打头，范围第1~94列
    lowPos = 161 + Math.abs(Random.nextInt(94))
    val bArr = new Array[Byte](2)
    bArr(0) = new Integer(highPos).byteValue
    bArr(1) = new Integer(lowPos).byteValue
    try
      nameCN = new String(bArr, "GB2312") //区位码组合成汉字
    catch {
      case e: UnsupportedEncodingException => e.printStackTrace()
    }
    nameCN
  }

  // 根据姓氏随机生成姓名
  def generateChineseName = {
    val random: Random = new Random(System.currentTimeMillis) // 此处输入你的姓氏，当然如果对姓氏没要求，可以输入多个，可以随机从这个多个姓氏中选择一个
    val Surname = Array("赵", "钱", "孙","李","周","吴","郑","王","冯","陈","楚","魏","韩")
    val index: Int = random.nextInt(Surname.length)
    var name = Surname(index) //获得一个随机的姓氏 /* 从常用字中选取一个或两个字作为名 */
    if (random.nextBoolean) {
      name += getChinese + getChinese
    } else {
      name += getChinese
    }
    name
  }

  def main(args: Array[String]): Unit = {
    val res2 = generateChineseName
    println(s"生成姓名为：${res2}")
  }
}
