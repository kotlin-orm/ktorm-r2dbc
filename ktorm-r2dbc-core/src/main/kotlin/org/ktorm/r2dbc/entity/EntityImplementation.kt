/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ktorm.r2dbc.entity

import org.ktorm.r2dbc.database.Database
import org.ktorm.r2dbc.schema.Table
import org.ktorm.r2dbc.schema.defaultValue
import org.ktorm.r2dbc.schema.kotlinProperty
import java.io.*
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.util.*
import kotlin.collections.LinkedHashMap
import kotlin.collections.LinkedHashSet
import kotlin.coroutines.Continuation
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.functions
import kotlin.reflect.jvm.javaGetter
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.jvm.jvmName
import kotlin.reflect.jvm.kotlinFunction

internal class EntityImplementation(
    var entityClass: KClass<*>,
    @Transient var fromDatabase: Database?,
    @Transient var fromTable: Table<*>?,
    @Transient var parent: EntityImplementation?
) : InvocationHandler, Serializable {

    var values = LinkedHashMap<String, Any?>()
    @Transient
    var changedProperties = LinkedHashSet<String>()

    private val doDeleteFun = this::doDelete
    private val doFlushChangeFun = this::doFlushChanges


    companion object {
        private const val serialVersionUID = 1L
        private val defaultImplsCache: MutableMap<Method, Method> = Collections.synchronizedMap(WeakHashMap())
    }

    override fun invoke(proxy: Any, method: Method, args: Array<out Any>?): Any? {
        return when (method.declaringClass.kotlin) {
            Any::class -> {
                when (method.name) {
                    "equals" -> this == args!![0]
                    "hashCode" -> this.hashCode()
                    "toString" -> this.toString()
                    else -> throw IllegalStateException("Unrecognized method: $method")
                }
            }
            Entity::class -> {
                when (method.name) {
                    "getEntityClass" -> this.entityClass
                    "getProperties" -> Collections.unmodifiableMap(this.values)
                    "discardChanges" -> this.doDiscardChanges()
                    "flushChanges" -> this.doFlushChangeFun.call(args!!.first())
                    "delete" -> this.doDeleteFun.call(args!!.first())
                    "get" -> this.values[args!![0] as String]
                    "set" -> this.doSetProperty(args!![0] as String, args[1])
                    "copy" -> this.copy()
                    else -> throw IllegalStateException("Unrecognized method: $method")
                }
            }
            else -> {
                handleMethodCall(proxy, method, args)
            }
        }
    }

    private fun handleMethodCall(proxy: Any, method: Method, args: Array<out Any>?): Any? {
        val ktProp = method.kotlinProperty
        if (ktProp != null) {
            val (prop, isGetter) = ktProp
            if (prop.isAbstract) {
                if (isGetter) {
                    val result = this.getProperty(prop, unboxInlineValues = true)
                    if (result != null || prop.returnType.isMarkedNullable) {
                        return result
                    } else {
                        return prop.defaultValue.also { cacheDefaultValue(prop, it) }
                    }
                } else {
                    this.setProperty(prop, args!![0])
                    return null
                }
            } else {
                return callDefaultImpl(proxy, method, args)
            }
        } else {
            val func = method.kotlinFunction
            if (func != null && !func.isAbstract) {
                return callDefaultImpl(proxy, method, args)
            } else {
                throw IllegalStateException("Unrecognized method: $method")
            }
        }
    }

    private val KProperty1<*, *>.defaultValue: Any
        get() {
            try {
                return javaGetter!!.returnType.defaultValue
            } catch (e: Throwable) {
                val msg = "" +
                        "The value of non-null property [$this] doesn't exist, " +
                        "an error occurred while trying to create a default one. " +
                        "Please ensure its value exists, or you can mark the return type nullable [${this.returnType}?]"
                throw IllegalStateException(msg, e)
            }
        }

    private fun cacheDefaultValue(prop: KProperty1<*, *>, value: Any) {
        val type = prop.javaGetter!!.returnType

        // Skip for primitive types, enums and string, because their default values always share the same instance.
        if (type == Boolean::class.javaPrimitiveType) return
        if (type == Char::class.javaPrimitiveType) return
        if (type == Byte::class.javaPrimitiveType) return
        if (type == Short::class.javaPrimitiveType) return
        if (type == Int::class.javaPrimitiveType) return
        if (type == Long::class.javaPrimitiveType) return
        if (type == String::class.java) return
        if (type.isEnum) return

        setProperty(prop, value)
    }

    @Suppress("SwallowedException")
    private fun callDefaultImpl(proxy: Any, method: Method, args: Array<out Any>?): Any? {
        val impl = defaultImplsCache.computeIfAbsent(method) {
            val cls = Class.forName(method.declaringClass.name + "\$DefaultImpls")
            cls.getMethod(method.name, method.declaringClass, *method.parameterTypes)
        }

        try {
            if (args == null) {
                return impl.invoke(null, proxy)
            } else {
                return impl.invoke(null, proxy, *args)
            }
        } catch (e: InvocationTargetException) {
            throw e.targetException
        }
    }

    fun hasProperty(prop: KProperty1<*, *>): Boolean {
        return prop.name in values
    }

    @OptIn(ExperimentalUnsignedTypes::class)
    fun getProperty(prop: KProperty1<*, *>, unboxInlineValues: Boolean = false): Any? {
        if (!unboxInlineValues) {
            return values[prop.name]
        }

        val returnType = prop.javaGetter!!.returnType
        val value = values[prop.name]

        // Unbox inline class values if necessary.
        // In principle, we need to check for all inline classes, but kotlin-reflect is still unable to determine
        // whether a class is inline, so as a workaround, we have to enumerate some common-used types here.
        return when {
            value is UByte && returnType == Byte::class.javaPrimitiveType -> value.toByte()
            value is UShort && returnType == Short::class.javaPrimitiveType -> value.toShort()
            value is UInt && returnType == Int::class.javaPrimitiveType -> value.toInt()
            value is ULong && returnType == Long::class.javaPrimitiveType -> value.toLong()
            value is UByteArray && returnType == ByteArray::class.java -> value.toByteArray()
            value is UShortArray && returnType == ShortArray::class.java -> value.toShortArray()
            value is UIntArray && returnType == IntArray::class.java -> value.toIntArray()
            value is ULongArray && returnType == LongArray::class.java -> value.toLongArray()
            else -> value
        }
    }

    @OptIn(ExperimentalUnsignedTypes::class)
    fun setProperty(prop: KProperty1<*, *>, value: Any?, forceSet: Boolean = false) {
        val propType = prop.returnType.jvmErasure

        // For inline classes, always box the underlying values as wrapper types.
        // In principle, we need to check for all inline classes, but kotlin-reflect is still unable to determine
        // whether a class is inline, so as a workaround, we have to enumerate some common-used types here.
        val boxedValue = when {
            propType == UByte::class && value is Byte -> value.toUByte()
            propType == UShort::class && value is Short -> value.toUShort()
            propType == UInt::class && value is Int -> value.toUInt()
            propType == ULong::class && value is Long -> value.toULong()
            propType == UByteArray::class && value is ByteArray -> value.toUByteArray()
            propType == UShortArray::class && value is ShortArray -> value.toUShortArray()
            propType == UIntArray::class && value is IntArray -> value.toUIntArray()
            propType == ULongArray::class && value is LongArray -> value.toULongArray()
            else -> value
        }

        doSetProperty(prop.name, boxedValue, forceSet)
    }

    private fun doSetProperty(name: String, value: Any?, forceSet: Boolean = false) {
        if (!forceSet && isPrimaryKey(name) && name in values) {
            val msg = "Cannot modify the primary key `$name` because it's already set to ${values[name]}"
            throw UnsupportedOperationException(msg)
        }

        values[name] = value
        changedProperties.add(name)
    }

    private fun copy(): Entity<*> {
        val entity = Entity.create(entityClass, parent, fromDatabase, fromTable)
        entity.implementation.changedProperties.addAll(changedProperties)

        for ((name, value) in values) {
            if (value is Entity<*>) {
                val valueCopy = value.copy()

                // Keep the parent relationship.
                if (valueCopy.implementation.parent == this) {
                    valueCopy.implementation.parent = entity.implementation
                }

                entity.implementation.values[name] = valueCopy
            } else {
                entity.implementation.values[name] = value?.let { deserialize(serialize(it)) }
            }
        }

        return entity
    }

    private fun serialize(obj: Any): ByteArray {
        ByteArrayOutputStream().use { buffer ->
            ObjectOutputStream(buffer).use { output ->
                output.writeObject(obj)
                output.flush()
                return buffer.toByteArray()
            }
        }
    }

    private fun deserialize(bytes: ByteArray): Any {
        ByteArrayInputStream(bytes).use { buffer ->
            ObjectInputStream(buffer).use { input ->
                return input.readObject()
            }
        }
    }

    private fun writeObject(output: ObjectOutputStream) {
        output.writeUTF(entityClass.jvmName)
        output.writeObject(values)
    }

    @Suppress("UNCHECKED_CAST")
    private fun readObject(input: ObjectInputStream) {
        entityClass = Class.forName(input.readUTF()).kotlin
        values = input.readObject() as LinkedHashMap<String, Any?>
        changedProperties = LinkedHashSet()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true

        return when (other) {
            is EntityImplementation -> entityClass == other.entityClass && values == other.values
            is Entity<*> -> entityClass == other.implementation.entityClass && values == other.implementation.values
            else -> false
        }
    }

    override fun hashCode(): Int {
        var result = 1
        result = 31 * result + entityClass.hashCode()
        result = 31 * result + values.hashCode()
        return result
    }

    override fun toString(): String {
        return entityClass.simpleName + values
    }
}
