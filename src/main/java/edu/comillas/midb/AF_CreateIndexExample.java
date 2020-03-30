package edu.comillas.midb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;

public class AF_CreateIndexExample {
    public static void main (String[] args){
        //TODO Crear un objeto cliente de mongo
        MongoClient mongo = new MongoClient("127.0.0.1", 27017);

        //TODO Obtener la base de datos "Empleo"
        //Se obtiene la base de datos con la que se va a trabajar. En este caso "Empleo"
        MongoDatabase db1 = mongo.getDatabase("Empleo");

        //TODO obtener la coleccion "Solicitudes"
        String collectionName = "Solicitudes";
        //Se obtiene la colleccion de documentos
        MongoCollection<Document> coll = db1.getCollection(collectionName);

        //Se crea un objeto indice con la restricción única
        IndexOptions indexOptions = new IndexOptions().unique(true);
        //Se crea el indice en la colleccion sobre los campos Codigo de mes y Codigo de Municipio.
        coll.createIndex(Indexes.ascending("Código_mes", "Codigo_Municipio"), indexOptions);

        //TODO Cerrar la conexión con la base de datos
        mongo.close();

    }
}
