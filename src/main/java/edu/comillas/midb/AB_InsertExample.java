package edu.comillas.midb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AB_InsertExample {
    public static void main(String[] args){
        //TODO Crear un objeto cliente de mongo
        MongoClient mongo = new MongoClient("127.0.0.1", 27017);

        //TODO Obtener la base de datos "Empleo"
        //Se obtiene la base de datos con la que se va a trabajar. En este caso "Empleo"
        MongoDatabase db1 = mongo.getDatabase("Empleo");

        //Se define la coleccion sobre la que se va a trabajar ."Test"
        String collectionName = "Test";
        //Se obtiene la colleccion de documentos
        MongoCollection<Document> collection = db1.getCollection(collectionName);
        //Se crea un documento nuevo
        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
                .append("info", new Document("x", 203).append("y", 102));

        //Se visualiza en formato Json el documento que se acaba de crear
        System.out.println(doc.toJson());

        //Se inserta el documento creado en la colección
        collection.insertOne(doc);




        //Para insertar de una sola vez muchos documentos creamos una lista de documentos
        List<Document> documents = new ArrayList<Document>();
        //Se crean 100 documentos y se añaden a la lista
        for (int i = 0; i < 100; i++) {
            documents.add(new Document("name", i)
                    .append("type", "database")
                    .append("count",i));
        }

        //Se inserta la lista
        collection.insertMany(documents);

        //TODO se cierra la conexion con la base de datos
        mongo.close();


    }
}
