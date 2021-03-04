object TestPath {

  def main(args: Array[String]): Unit = {

    //只能获取类根目录下的所有文件
    val path = getClass.getResource("../../../data/carFlow_all_column_test.txt").getPath
    println(path)
  }
}
