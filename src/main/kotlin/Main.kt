package org.example

import org.postgresql.util.PSQLException
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet


data class Alumnos (val dni:String,val apenom:String,val direc:String,val pobla:String,val telef:String) {
    constructor(): this("","","","","")
}
data class Assignaturas(val cod:Int, val nombre:String)
data class Notas(val dni:String, val cod:Int, val nota:Int)

fun main() {
    val jdbcUrl = "jdbc:postgresql://localhost:5432/school"
    try {
        val pepe = DatabaseSQLController()
        pepe.connect(jdbcUrl,"sjo","")
        pepe.getTabla("alumnos")
        do {
            val alumn = pepe.next<Alumnos>()
            println(alumn!!.dni)
        } while(pepe.hasNext())
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

    inline fun <reified T> next(): T? {
        if (result.next()) {
            val parametres = mutableListOf<Any>()
            for (i in 0 until T::class.typeParameters.size) {
                val parametre:Any = when(T::class.typeParameters[i].name) {
                    "Int" -> result.getInt(i)
                    "String" -> result.getString(i)
                    "Float" -> result.getFloat(i)
                    "Boolean" -> result.getBoolean(i)
                    else -> ""
                }
                parametres.add(parametre)
            }

            return T::class.constructors.first().call(parametres.toTypedArray())
        }
        return null
    }

    fun disconnect() {
        result.close()
        this.connection.close()
    }
}
