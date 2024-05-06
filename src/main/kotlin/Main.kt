package org.example

import org.postgresql.util.PSQLException
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet


data class Alumnos(val dni:String,val apenom:String,val direc:String,val pobla:String,val telef:String)
data class Assignaturas(val cod:Int, val nombre:String)
data class Notas(val dni:String, val cod:Int, val nota:Int)

fun main() {
    val jdbcUrl = "jdbc:postgresql://localhost:5432/school"
    try {
    } catch (e: PSQLException) {
        println(e.message)
    }
}


class DatabaseMONGOController {


    fun insertData() {

    }

    fun insertSingledata() {

    }

    fun connectColection() {

    }

    fun connect() {

    }
}


class DatabaseSQLController {
    private lateinit var connection:Connection
    private lateinit var result:ResultSet


    fun connect(jdbcUrl:String, user:String, password:String) {
        this.connection = DriverManager.getConnection(jdbcUrl, user, password)
    }

    fun getTabla(nomTaula:String) {
        val statement = connection.createStatement()
        this.result = statement.executeQuery("SELECT * FROM $nomTaula'")
    }

    fun next(columnaType:Any, columnName:String): Any? {
        if (this.result.next()) {
            when (columnaType) {
                is Int -> return this.result.getInt(columnName)
                is String -> return this.result.getString(columnName)
                is Float -> return this.result.getFloat(columnName)
                is Boolean -> return this.result.getBoolean(columnName)
            }
        }
        return null
    }

    fun disconnect() {
        result.close()
        this.connection.close()
    }
}
