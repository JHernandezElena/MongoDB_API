package edu.comillas.midb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class AA_ConectionExample {
    public static void main(String[] args){
        //Se crea un objeto cliente de la base de datos con la IP y el puerto
        MongoClient mongo = new MongoClient("127.0.0.1", 27017);
        //Se obtiene la base de datos con la que se va a trabajar. En este caso "Empleo"
        MongoDatabase db1 = mongo.getDatabase("Empleo");
        //Se cierra la conexión con la base de datos
        mongo.close();

    }
}
