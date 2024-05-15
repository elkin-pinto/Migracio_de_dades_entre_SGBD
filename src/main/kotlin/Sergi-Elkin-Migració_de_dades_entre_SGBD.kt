package org.example

// Importaciones de las librerías necesarias
import com.mongodb.MongoException
import com.mongodb.MongoTimeoutException
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.bson.Document
import org.postgresql.util.PSQLException
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import kotlin.reflect.full.memberProperties

// Definición de las clases de datos para representar entidades del sistema
data class Alumnos(val dni: String, val apenom: String, val direc: String, val pobla: String, val telef: String)
data class Assignaturas(val cod: Int, val nombre: String)
data class Notas(val dni: String, val cod: Int, val nota: Int)

fun main() {
    // Definición de los hosts de PostgreSQL y MongoDB
    val postgresHost = "localhost:5432"
    val mongoHost = "sergioherrador.bwwhoy4.mongodb.net/?retryWrites=true&w=majority&appName=SergioHerrador"

    // Tenemos las tablas que queremos migrar
    val tablas: MutableList<String> = mutableListOf("alumnos", "notas", "asignaturas")

    // Variables para las conexiones a PostgreSQL y MongoDB
    var postgres: Postgres? = null
    var mongo: Mongo? = null
    try {
        // Instanciamos los objetos de postgres y mongo
        postgres = Postgres()
        mongo = Mongo()

        // Nos conectamos a PostgreSQL y comenzamos a leer
        postgres.connexioBD(postgresHost, "sjo", "", "school")
        for (tabla in tablas) {
            postgres.llegeix(tabla)
        }

        // Nos conectamos a MongoDB
        mongo.connexioBD(mongoHost, "elkin", "pepoClown123", "itb")

        // Iteramos sobre
        for (tabla in tablas) {
            postgres.llegeix(tabla) // Leemos la tabla y modificamos su resultSet
            while (postgres.hiha(tabla) == true) { // Leemos si hay resultados en la tabla que buscamos, si es asi iteramos
                when (tabla) {
                    "alumnos" -> {
                        val alumn = postgres.recupera<Alumnos>(tabla) // Obtenemos un alumno
                        if (alumn != null) { // Si el alumno no es null imprimimos los datos y lo insertamos en MongoDB
                            println(alumn.dni)
                            println(alumn.apenom)
                            println(alumn.direc)
                            println(alumn.pobla)
                            println(alumn.telef)
                            mongo.insereix(tabla, alumn)
                        } else {
                            println("El alumno es null, no se ha encontrado ninguno")
                        }
                    }

                    "notas" -> {
                        val nota = postgres.recupera<Notas>(tabla) // Obtenemos una nota
                        if (nota != null) { // Si la nota no es null imprimimos los datos y la insertamos en MongoDB
                            println(nota.dni)
                            println(nota.nota)
                            println(nota.cod)
                            mongo.insereix(tabla, nota)
                        } else {
                            println("La nota es null, no se ha encontrado ninguna")
                        }
                    }

                    "asignatura" -> {
                        val assignatura = postgres.recupera<Assignaturas>(tabla) // Obtenemos una asignatura
                        if (assignatura != null) { // Si la asignatura no es null imprimimos los datos y la insertamos en MongoDB
                            println(assignatura.nombre)
                            println(assignatura.cod)
                            mongo.insereix(tabla, assignatura)
                        } else {
                            println("La asignatura es null, no se ha encontrado ninguna")
                        }
                    }
                }
            }
        }

    } catch (e: PSQLException) {
        println(e.message)
    } finally {
        // Finalmente nos desconectamos de las bases de datos
        postgres?.desconnexioBD()
        mongo?.desconnexioBD()
    }
}

// Clase para manejar las operaciones con PostgreSQL
class Postgres {
    private lateinit var connection: Connection
    var result = mutableMapOf<String, ResultSet>()

    // Establece la conexión contra la BD del servidor PostgreSQL
    fun connexioBD(host: String, user: String, password: String, bd: String) {
        val jdbcUrl = "jdbc:postgresql://$host/$bd"
        this.connection = DriverManager.getConnection(jdbcUrl, user, password)
        llegeix(bd)
    }

    // Inicia la lectura de la tabla recibida como parámetro
    fun llegeix(nomTaula: String) {
        val statement = connection.createStatement()
        this.result[nomTaula] = statement.executeQuery("SELECT * FROM $nomTaula")
    }

    // Retorna un booleano si hay datos disponibles para leer de la tabla recibida como parámetro
    fun hiha(nomTaula: String): Boolean? {
        return this.result[nomTaula]?.next()
    }

    // Retorna un objeto con los datos leídos de la tabla recibida como parámetro.
    inline fun <reified T : Any> recupera(nomTaula: String): T? {
        if (result[nomTaula]?.next() == true) { // Si el resultado tiene siguiente lo obtenemos
            val parametres = mutableListOf<Any>()
            val properties = T::class.memberProperties
            for ((index, property) in properties.withIndex()) {
                // Obtenemos los parámetros y el índice, para cada parámetro verificamos su tipo y lo obtenemos
                val parametre: Any? = when (property.returnType.toString()) {
                    "kotlin.String" -> result[nomTaula]!!.getString(index + 1)
                    "kotlin.Int" -> result[nomTaula]!!.getInt(index + 1)
                    "kotlin.Float" -> result[nomTaula]!!.getFloat(index + 1)
                    "kotlin.Boolean" -> result[nomTaula]!!.getBoolean(index + 1)
                    else -> "" // Puedes manejar otros tipos si es necesario
                }
                parametres.add(parametre ?: "")
            }
            // Una vez tenemos todos los parámetros con estos creamos una instancia del objeto con su constructor primario
            return T::class.constructors.first().call(*parametres.toTypedArray())
        }
        return null // Si en el resultado no hay siguiente retornamos null
    }

    // Cierra la conexión del servidor PostgreSQL.
    fun desconnexioBD() {
        for (result in this.result) {
            result.value.close()
        }
        this.connection.close()
    }
}

// Clase para manejar las operaciones con MongoDB
class Mongo {
    private var mongoClient: MongoClient? = null // Empieza como null y en la conexion cambia el valor
    private var baseDades: MongoDatabase? = null // Empieza como null y en la conexion cambia el valor

    // Establece la conexión contra la BD del servidor MongoDB
    fun connexioBD(host: String, usuari: String, password: String, bd: String) {
        try {
            // Conectarse al servidor de MongoDB
            val mongoClientFun = mongoClient

            val mongoUrl = "mongodb+srv://$usuari:$password@$host"

            // Crear conexión
            mongoClient = MongoClients.create(mongoUrl)

            // Obtener la base de datos deseada y cambiar el parametro de la clase
            baseDades = mongoClientFun?.getDatabase(bd)
        } catch (e: MongoTimeoutException) {
            println("No se pudo conectar a la base de datos")
        } catch (e: MongoException) {
            println(e.message)
        } catch (e: Exception) {
            println(e.message)
        }
    }

    // Inserta el documento correspondiente al objeto recibido como parámetro en la colección indicada
    fun insereix(colleccio: String, objecte: Any) {
        if (baseDades == null) { // si es null significa que no se ha usado la función connexioBD
            println("Primero debes hacer la función connexioBD")
        } else {
            val coll: MongoCollection<Document> = baseDades!!.getCollection(colleccio) // Obtenemos la conexión
            val objecteJsonString = Json.encodeToString(objecte) // Codificamos el tipo genérico en un JSON STRING
            coll.insertOne(Document.parse(objecteJsonString)) // y una vez con el json string lo insertamos como documento usando parse
        }

    }

    // Cierra la conexión del servidor MongoDB
    fun desconnexioBD() {
        if (mongoClient != null) {
            mongoClient!!.close()
            println("Desconexión completada")
        } else {
            println("Estás desconectado")
        }
    }
}