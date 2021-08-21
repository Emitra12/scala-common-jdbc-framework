package com.sa.alrajhi

import java.io.Closeable
import scala.io.Source

package object Helper {
  def autoClose[A <: Closeable, B](closeable: A)(closure: A => B): B = {
    try {
      closure(closeable)
    } finally {
      if(closeable != null) {
        closeable.close()
      }
    }
  }

  def autoCloseSource[A <: Source, B](closeable: A)(closure: A => B): B = {
    try {
      closure(closeable)
    } finally {
      if(closeable != null) {
        closeable.close()
      }
    }
  }
}