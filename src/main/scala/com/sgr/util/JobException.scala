package com.sgr.util

/**
 * @author sunguorui
 * @date 2021年10月18日 8:11 下午
 */
case class JobException(msg: String, errorCode: Int = 1) extends Exception(msg: String)
// 异常错误代码定义
object JobException {

}
