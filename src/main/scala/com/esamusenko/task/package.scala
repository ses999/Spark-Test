package com.esamusenko

package object task {
  type Closeable = {def close(): Unit}

  def using[A <: Closeable, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    }
    finally {
      resource.close()
    }
  }


}
