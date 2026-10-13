/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron

import java.io.{ByteArrayOutputStream, InputStream, ObjectOutputStream}
import java.lang.reflect.{InvocationHandler, Method, Proxy}

import org.apache.spark.sql.AuronQueryTest
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, StringType}

import org.apache.auron.protobuf.ScalarFunction

class NativeConvertersSuite
    extends AuronQueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {

  private def assertTrimmedCast(rawValue: String, targetType: DataType): Unit = {
    val expr = Cast(Literal.create(rawValue, StringType), targetType)
    val nativeExpr = NativeConverters.convertExpr(expr)

    assert(nativeExpr.hasTryCast)
    val childExpr = nativeExpr.getTryCast.getExpr
    assert(childExpr.hasScalarFunction)
    val scalarFn = childExpr.getScalarFunction
    assert(scalarFn.getFun == ScalarFunction.Trim)
    assert(scalarFn.getArgsCount == 1 && scalarFn.getArgs(0).hasLiteral)
  }

  private def assertNonTrimmedCast(rawValue: String, targetType: DataType): Unit = {
    val expr = Cast(Literal.create(rawValue, StringType), targetType)
    val nativeExpr = NativeConverters.convertExpr(expr)

    assert(nativeExpr.hasTryCast)
    val childExpr = nativeExpr.getTryCast.getExpr
    assert(!childExpr.hasScalarFunction)
    assert(childExpr.hasLiteral)
  }

  test("cast from string to numeric adds trim wrapper before native cast when enabled") {
    withSQLConf("spark.auron.cast.trimString" -> "true") {
      assertTrimmedCast(" 42 ", IntegerType)
    }
  }

  test("cast from string to boolean adds trim wrapper before native cast when enabled") {
    withSQLConf("spark.auron.cast.trimString" -> "true") {
      assertTrimmedCast(" true ", BooleanType)
    }
  }

  test("cast trim disabled via auron conf") {
    withSQLConf("spark.auron.cast.trimString" -> "false") {
      assertNonTrimmedCast(" 42 ", IntegerType)
    }
  }

  test("cast trim disabled via auron conf for boolean cast") {
    withSQLConf("spark.auron.cast.trimString" -> "false") {
      assertNonTrimmedCast(" true ", BooleanType)
    }
  }

  test("cast with non-string child remains unchanged") {
    val expr = Cast(Literal(1.5), IntegerType)
    val nativeExpr = NativeConverters.convertExpr(expr)

    assert(nativeExpr.hasTryCast)
    val childExpr = nativeExpr.getTryCast.getExpr
    assert(!childExpr.hasScalarFunction)
    assert(childExpr.hasLiteral)
  }

  /** Writes the expression/payload pair that deserializeExpression expects to read back. */
  private def serializeObjects(expr: AnyRef, payload: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    try {
      oos.writeObject(expr)
      oos.writeObject(payload)
    } finally {
      oos.close()
    }
    bos.toByteArray
  }

  test("deserializeExpression resolves proxy interfaces with the pinned class loader") {
    val appLoader = getClass.getClassLoader
    val interfaceName = classOf[ProxiedPayload].getName
    // redefines only ProxiedPayload, so a proxy resolved through this loader carries an
    // interface Class distinct from the one the application class loader hands out
    val pinnedLoader = new SingleClassRedefiningLoader(interfaceName, appLoader)

    val proxy: AnyRef = Proxy.newProxyInstance(
      appLoader,
      Array[Class[_]](classOf[ProxiedPayload]),
      new ConstantPayloadHandler("auron"))
    val serialized = serializeObjects(Literal(1), proxy)

    val previousLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(pinnedLoader)
    try {
      val (_, payload) =
        NativeConverters.deserializeExpression[Literal, java.io.Serializable](serialized)
      val resolvedInterface = payload.getClass.getInterfaces
        .find(_.getName == interfaceName)
        .getOrElse(fail(s"deserialized proxy does not implement $interfaceName"))
      assert(
        resolvedInterface.getClassLoader eq pinnedLoader,
        "proxy interface was resolved by a class loader taken from the call stack " +
          s"(${resolvedInterface.getClassLoader}) instead of the pinned loader")
    } finally {
      Thread.currentThread().setContextClassLoader(previousLoader)
    }
  }

  test("deserializeExpression resolves primitive type descriptors") {
    // no class loader resolves "int" by name, so this only works via the default fallback
    val serialized = serializeObjects(Literal(1), java.lang.Integer.TYPE)
    val (_, payload) = NativeConverters.deserializeExpression[Literal, Class[_]](serialized)
    assert(payload eq java.lang.Integer.TYPE)
  }
}

/** Serializable interface backing the dynamic proxy used by [[NativeConvertersSuite]]. */
trait ProxiedPayload extends java.io.Serializable {
  def payload(): String
}

/** Serializable [[InvocationHandler]] so the proxy itself can be written to a stream. */
class ConstantPayloadHandler(value: String) extends InvocationHandler with java.io.Serializable {
  override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef =
    method.getName match {
      case "payload" => value
      case "toString" => s"ProxiedPayload($value)"
      case "hashCode" => Integer.valueOf(value.hashCode)
      case "equals" => java.lang.Boolean.valueOf(proxy eq args(0))
      case other => throw new UnsupportedOperationException(other)
    }
}

/**
 * Defines one named class from its own bytes and delegates every other name to the parent, so
 * that class resolves to a Class object distinct from the parent's copy.
 */
class SingleClassRedefiningLoader(targetName: String, parent: ClassLoader)
    extends ClassLoader(parent) {

  override def loadClass(name: String, resolve: Boolean): Class[_] = synchronized {
    if (name != targetName) {
      return super.loadClass(name, resolve)
    }
    val loaded = findLoadedClass(name)
    val cls = if (loaded != null) loaded else defineFromParent(name)
    if (resolve) {
      resolveClass(cls)
    }
    cls
  }

  private def defineFromParent(name: String): Class[_] = {
    val resource = name.replace('.', '/') + ".class"
    val in = parent.getResourceAsStream(resource)
    require(in != null, s"cannot read class bytes of $name")
    try {
      val bytes = readFully(in)
      defineClass(name, bytes, 0, bytes.length)
    } finally {
      in.close()
    }
  }

  private def readFully(in: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val buf = new Array[Byte](8192)
    var read = in.read(buf)
    while (read >= 0) {
      out.write(buf, 0, read)
      read = in.read(buf)
    }
    out.toByteArray
  }
}
