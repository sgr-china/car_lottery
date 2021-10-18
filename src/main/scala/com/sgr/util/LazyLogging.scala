package com.sgr.util

import org.slf4j.{Logger, LoggerFactory}

import javax.ws.rs.ext.ParamConverter.Lazy


trait LazyLogging {
  protected lazy val logger: Logger =
    LoggerFactory.getLogger("car—lottery")

}
