package org.example

import org.postgresql.util.PSQLException
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import kotlin.reflect.full.memberProperties


data class Alumnos (val dni:String,val apenom:String,val direc:String,val pobla:String,val telef:String)
data class Assignaturas(val cod:Int, val nombre:String)
data class Notas(val dni:String, val cod:Int, val nota:Int)

fun main() {
    val jdbcUrl = "jdbc:postgresql://localhost:5432/school"
    try {
        val pepe = DatabaseSQLController()
        pepe.connect(jdbcUrl,"sjo","")
        pepe.getTabla("alumnos")
        while(pepe.hasNext()) {
            val alumn = pepe.next<Alumnos>()
            println(alumn!!.dni)
        }
    } catch (e: PSQLException) {
        println(e.message)
    }
}

class DatabaseSQLController {
    private lateinit var connection:Connection
    lateinit var result:ResultSet


    fun connect(jdbcUrl:String, user:String, password:String) {
        this.connection = DriverManager.getConnection(jdbcUrl, user, password)
    }


    fun getTabla(nomTaula:String) {
        val statement = connection.createStatement()
        this.result = statement.executeQuery("SELECT * FROM $nomTaula")
    }

    fun hasNext(): Boolean {
        return this.result.next()
    }

    inline fun <reified T:Any> next(): T? {
        if (result.next()) {
            val parametres = mutableListOf<Any>()
            val properties = T::class.memberProperties
            for (property in properties) {
                val columnName = property.name
                val parametre: Any? = when (property.returnType.toString()) {
                    "kotlin.String" -> result.getString(columnName)
                    "kotlin.Int" -> result.getInt(columnName)
                    "kotlin.Float" -> result.getFloat(columnName)
                    "kotlin.Boolean" -> result.getBoolean(columnName)
                    else -> "" // Puedes manejar otros tipos si es necesario
                }
                parametres.add(parametre ?: "")
            }
            return T::class.constructors.first().call(*parametres.toTypedArray())
        }
        return null
    }

    fun disconnect() {
        result.close()
        this.connection.close()
    }
}

