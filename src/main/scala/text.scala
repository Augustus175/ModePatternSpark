/**
 * Created by zhangzhibo on 17-3-16.
 */
class text {
  def editdistance(s1: String, s2: String): Float = {
//    val str1 = "abc"
    val str1 = s1
//    val str1 = "abc"
    val str2 = s2
    val len1 = str1.length
    val len2 = str2.length
    val edit = Array.ofDim[Int](len1+1,len2+1)

    for (i <- 0 until edit.length) {
      edit(i)(0) = i
    }
    for (i <- 0 until edit(0).length) {
      edit(0)(i) = i
    }


    var tmp = 0
    for (i <- 1 to len1) {
      for (j <- 1 to len2) {
        if (str1.charAt(i - 1) == str2.charAt(j - 1)) {
          tmp = 0
        } else {
          tmp = 1
        }
        edit(i)(j) = Math.min(edit(i - 1)(j - 1) + tmp, Math.min(edit(i)(j - 1) + 1, edit(i - 1)(j) + 1))
      }
    }
    for (i <- 0 until edit.length) {
      for (j <- 0 until edit(i).length) {
        print(edit(i)(j) + " ")
      }
      println
    }
    val tmp1: Float = edit(len1)(len2)
    val tmp2: Float = Math.max(len1, len2)
    val similarity = 1.0F - tmp1 / tmp2
//    println(tmp1)
//    println(tmp2)
//    println(similarity)
    return similarity
  }


}
