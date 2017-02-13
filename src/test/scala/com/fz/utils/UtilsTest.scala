package com.fz.utils

import java.io.File

import com.fz.util.Utils
import org.junit.Assert._
import org.junit.Test

/**
 * 测试Utils工具类
 * Created by fanzhe on 2017/1/25.
 */
@Test
class UtilsTest {

  @Test
  def testGetModel()={
    val path = "/user/algorithm/model/logistic/output00"
    val sc = Utils.getSparkContext(true,"local")
    val model = Utils.getModel(sc, path)
    assertTrue(model != null)
  }

}
