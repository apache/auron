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
package org.apache.auron.utils

import java.util

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Test settings which can enable/disable some tests on demand(e.g. Auron has some correctness bug
 * that not fixed yet).
 */
abstract class SparkTestSettings {
  private val AURON_TEST: String = "Auron - "
  private val enabledSuites: java.util.Map[String, SuiteSettings] = new util.HashMap()

  protected def enableSuite[T: ClassTag]: SuiteSettings = {
    enableSuite(implicitly[ClassTag[T]].runtimeClass.getCanonicalName)
  }

  protected def enableSuite(suiteName: String): SuiteSettings = {
    if (enabledSuites.containsKey(suiteName)) {
      throw new IllegalArgumentException("Duplicated suite name: " + suiteName)
    }
    val suiteSettings = new SuiteSettings
    enabledSuites.put(suiteName, suiteSettings)
    suiteSettings
  }

  private[utils] def shouldRun(suiteName: String, testName: String): Boolean = {
    if (!enabledSuites.containsKey(suiteName)) {
      return false
    }

    val suiteSettings = enabledSuites.get(suiteName)
    suiteSettings.disableReason match {
      case Some(_) => return false
      case _ => // continue
    }

    val inclusion = suiteSettings.inclusion.asScala
    val exclusion = suiteSettings.exclusion.asScala

    if (inclusion.isEmpty && exclusion.isEmpty) {
      // default to run all cases under this suite
      return true
    }

    if (inclusion.nonEmpty && exclusion.nonEmpty) {
      // error
      throw new IllegalStateException(
        s"Do not use include and exclude conditions on the same test case: $suiteName:$testName")
    }

    if (inclusion.nonEmpty) {
      // include mode
      val isIncluded = inclusion.exists(_.isIncluded(testName))
      return isIncluded
    }

    if (exclusion.nonEmpty) {
      // exclude mode
      val isExcluded = exclusion.exists(_.isExcluded(testName))
      return !isExcluded
    }

    throw new IllegalStateException("Unreachable code from shouldRun")
  }

  /**
   * Settings for each test suite.
   *
   * Each test suite is consists of many tests. With SuiteSettings we can control which test to
   * include/exclude.
   */
  final protected class SuiteSettings {
    private[utils] val inclusion: util.List[IncludeBase] = new util.ArrayList()
    private[utils] val exclusion: util.List[ExcludeBase] = new util.ArrayList()

    private[utils] var disableReason: Option[String] = None

    def include(testNames: String*): SuiteSettings = {
      inclusion.add(Include(testNames: _*))
      this
    }

    def exclude(testNames: String*): SuiteSettings = {
      exclusion.add(Exclude(testNames: _*))
      this
    }

    def includeByPrefix(prefixes: String*): SuiteSettings = {
      inclusion.add(IncludeByPrefix(prefixes: _*))
      this
    }
    def excludeByPrefix(prefixes: String*): SuiteSettings = {
      exclusion.add(ExcludeByPrefix(prefixes: _*))
      this
    }

    def disable(reason: String): SuiteSettings = {
      disableReason = disableReason match {
        case Some(r) => throw new IllegalArgumentException("Disable reason already set: " + r)
        case None => Some(reason)
      }
      this
    }
  }

  object SuiteSettings {
    implicit class SuiteSettingsImplicits(settings: SuiteSettings) {
      def includeAuronTest(testName: String*): SuiteSettings = {
        settings.include(testName.map(AURON_TEST + _): _*)
        settings
      }

      def excludeAuronTest(testName: String*): SuiteSettings = {
        settings.exclude(testName.map(AURON_TEST + _): _*)
        settings
      }

      def includeAuronTestsByPrefix(prefixes: String*): SuiteSettings = {
        settings.includeByPrefix(prefixes.map(AURON_TEST + _): _*)
        settings
      }

      def excludeAuronTestsByPrefix(prefixes: String*): SuiteSettings = {
        settings.excludeByPrefix(prefixes.map(AURON_TEST + _): _*)
        settings
      }

      def includeAllAuronTests(): SuiteSettings = {
        settings.include(AURON_TEST)
        settings
      }

      def excludeAllAuronTests(): SuiteSettings = {
        settings.exclude(AURON_TEST)
        settings
      }
    }
  }

  protected trait IncludeBase {
    def isIncluded(testName: String): Boolean
  }

  protected trait ExcludeBase {
    def isExcluded(testName: String): Boolean
  }

  private case class Include(testNames: String*) extends IncludeBase {
    val nameSet: Set[String] = Set(testNames: _*)
    override def isIncluded(testName: String): Boolean = nameSet.contains(testName)
  }

  private case class Exclude(testNames: String*) extends ExcludeBase {
    val nameSet: Set[String] = Set(testNames: _*)
    override def isExcluded(testName: String): Boolean = nameSet.contains(testName)
  }

  private case class IncludeByPrefix(prefixes: String*) extends IncludeBase {
    override def isIncluded(testName: String): Boolean = {
      if (prefixes.exists(prefix => testName.startsWith(prefix))) {
        return true
      }
      false
    }
  }

  private case class ExcludeByPrefix(prefixes: String*) extends ExcludeBase {
    override def isExcluded(testName: String): Boolean = {
      if (prefixes.exists(prefix => testName.startsWith(prefix))) {
        return true
      }
      false
    }
  }

  def getSQLQueryTestSettings: SQLQueryTestSettings
}

object SparkTestSettings {
  val instance: SparkTestSettings = Class
    .forName("org.apache.auron.utils.AuronSparkTestSettings")
    .getDeclaredConstructor()
    .newInstance()
    .asInstanceOf[SparkTestSettings]

  def shouldRun(suiteName: String, testName: String): Boolean = {
    instance.shouldRun(suiteName, testName: String)
  }
}
