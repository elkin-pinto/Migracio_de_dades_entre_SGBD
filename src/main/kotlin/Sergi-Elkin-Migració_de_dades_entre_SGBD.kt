package org.example

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


data class Alumnos(val dni: String, val apenom: String, val direc: String, val pobla: String, val telef: String)
data class Assignaturas(val cod: Int, val nombre: String)
data class Notas(val dni: String, val cod: Int, val nota: Int)

fun main() {
    val postgresHost = "localhost:5432"
    val mongoHost = "sergioherrador.bwwhoy4.mongodb.net/?retryWrites=true&w=majority&appName=SergioHerrador"

    val tablas : MutableList<String> = mutableListOf("alumnos","notas","asignaturas")

    var postgres: Postgres? = null
    var mongo: Mongo? = null
    try {
        // Instanciem els objectes de postgres i mongo
        postgres = Postgres()
        mongo = Mongo()

        // Ens conectem a postgres i comencem a llegir
        postgres.connexioBD(postgresHost, "sjo", "", "school")
        postgres.llegeix("alumnos")

        // Ens conectem a mongo
        mongo.connexioBD(mongoHost, "elkin", "pepoClown123", "itb")

        for (tabla in tablas) {
            postgres.llegeix(tabla) // Leemos la tabla que toque
            while (postgres.hiha()) {
                when (tabla) {
                    "alumnos" -> {
                        val alumn = postgres.recupera<Alumnos>() // Agafem un alumne
                        if (alumn != null) { // Si l'alumne no es null imprimim les dades i el pujem a mongo
                            println(alumn.dni)
                            println(alumn.apenom)
                            println(alumn.direc)
                            println(alumn.pobla)
                            println(alumn.telef)
                            mongo.insereix("alumnos", alumn)
                        } else {
                            println("L'alumne es null, no s'ha trobat cap")
                        }
                    }

                    "notas" -> {
                        val nota = postgres.recupera<Notas>() // Agafem un alumne
                        if (nota != null) { // Si la nota no es null imprimim les dades i el pujem a mongo
                            println(nota.dni)
                            println(nota.nota)
                            println(nota.cod)
                            mongo.insereix("notas", nota)
                        } else {
                            println("La nota es null, no s'ha trobat cap")
                        }
                    }

                    "asignatura" -> {
                        val assignatura = postgres.recupera<Assignaturas>() // Agafem un alumne
                        if (assignatura != null) { // Si la assignatura no es null imprimim les dades i el pujem a mongo
                            println(assignatura.nombre)
                            println(assignatura.cod)
                            mongo.insereix("asignaturas", assignatura)
                        } else {
                            println("La nota es null, no s'ha trobat cap")
                        }
                    }
                }
            }
        }

    } catch (e: PSQLException) {
        println(e.message)
    } finally {
        // Finalment es desconectem de les bases de dades
        postgres?.desconnexioBD()
        mongo?.desconnexioBD()
    }
}

class Postgres {
    private lateinit var connection: Connection
    lateinit var result: ResultSet

    // Estableix la connexió contra la BD del servidor  PostgreSQL.
    fun connexioBD(host: String, user: String, password: String, bd: String) {
        val jdbcUrl = "jdbc:postgresql://$host/$bd"
        this.connection = DriverManager.getConnection(jdbcUrl, user, password)
        llegeix(bd)
    }

    // inicia l’operació de lectura de la taula rebuda com a paràmetre.
    fun llegeix(nomTaula: String) {
        val statement = connection.createStatement()
        this.result = statement.executeQuery("SELECT * FROM $nomTaula")
    }

    // Indica si hi ha dades disponibles per llegir de la taula rebuda com a paràmetre.
    fun hiha(): Boolean {
        return this.result.next()
    }

    // Retorna un objecte amb les dades llegides de la taula rebuda com a paràmetre.
    inline fun <reified T : Any> recupera(): T? {
        if (result.next()) { // Si el resultat te seguent el agafem
            val parametres = mutableListOf<Any>()
            val properties = T::class.memberProperties
            for ((index, property) in properties.withIndex()) {
                // Agafem els parametres i el index, per cada parametre verifiquem que tipus es i el agafem
                val parametre: Any? = when (property.returnType.toString()) {
                    "kotlin.String" -> result.getString(index + 1)
                    "kotlin.Int" -> result.getInt(index + 1)
                    "kotlin.Float" -> result.getFloat(index + 1)
                    "kotlin.Boolean" -> result.getBoolean(index + 1)
                    else -> "" // Puedes manejar otros tipos si es necesario
                }
                parametres.add(parametre ?: "")
            }
            // Un cop tenims tots els parametres amb aquests creem una instancia del objecte amb el seu constructor primari
            return T::class.constructors.first().call(*parametres.toTypedArray())
        }
        return null // Si en el resultat no hi ha seguent retornem null
    }

    // Fa la desconnexió del servidor PostgreSQL.
    fun desconnexioBD() {
        result.close()
        this.connection.close()
    }
}

class Mongo {
    private var colleccio: String = ""
    private var host: String = ""
    private var usuari: String = ""
    private var password: String = ""
    private var bd: String = ""
    private var mongoClient: MongoClient? = null
    private var baseDades: MongoDatabase? = null

    // Estableix la connexió contra la BD del servidor MongoDB
    fun connexioBD(host: String, usuari: String, password: String, bd: String) {
        this.host = host
        this.usuari = usuari
        this.password = password
        this.bd = bd

        try {
            // Connectar-se a servidor de mongoDB
            val mongoClientFun = mongoClient

            val mongoUrl = "mongodb+srv://$usuari:$password@$host"
            // Crear connexió
            mongoClient = MongoClients.create(mongoUrl)

            // Agafar la base de dades desitjada
            baseDades = mongoClientFun?.getDatabase(bd)
        } catch (e: MongoTimeoutException) {
            println("No arribem a la base de dades")
        } catch (e: MongoException) {
            println(e.message)
        } catch (e: Exception) {
            println(e.message)
        }
    }

    // Insereix el document corresponent a l’objecte rebut com a paràmetre a la col·lecció indicada
    fun insereix(colleccio: String, objecte: Any) {
        this.colleccio = colleccio // Agafem la collecio y la afegim com a variable de la nostra classe
        if (baseDades == null) { // si es null significa que no s'ha fet servir la funció connexioBD
            println("Primer has de fer la funcio connexioBD")
        } else {
            val coll: MongoCollection<Document> = baseDades!!.getCollection(colleccio) // Agafem la connexió
            val objecteJsonString = Json.encodeToString(objecte) // Encodifiquem el tipus genèric
            coll.insertOne(Document.parse(objecteJsonString)) // i una vegada amb el json string l'insertem
        }

    }

    // Fa la desconnexió del servidor MongoDB
    fun desconnexioBD() {
        if (mongoClient != null) {
            mongoClient!!.close()
            println("Desconexxió completada")
        } else {
            println("Estas desconectat")
        }
    }
}