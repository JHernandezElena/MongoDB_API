package edu.comillas.midb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;



public class AE_AggregateExample {
    public static void main(String[] args){
        //TODO Crear un objeto cliente de mongo
        MongoClient mongo = new MongoClient("127.0.0.1", 27017);

        //TODO Obtener la base de datos "Empleo"
        //Se obtiene la base de datos con la que se va a trabajar. En este caso "Empleo"
        MongoDatabase db1 = mongo.getDatabase("Empleo");

        //TODO obtener la coleccion "Solicitudes"
        String collectionName = "Solicitudes";
        //Se obtiene la colleccion de documentos
        MongoCollection<Document> coll = db1.getCollection(collectionName);

        //Se crea un documento con un Json para crear un campo que multiplique el total de demandantes por dos
        Document multiply = new Document("$multiply", Arrays.asList("$Total_Demandantes", 2));

        //Se crea una lista donde ir almacenando los diferentes pasos del proceso
        List<Bson> query = new ArrayList<Bson>();

        //Se añade como primer estado del pipeline un filtro para elegir sólo entre enero y junio
        query.add(Aggregates.match(and(lte("Código_mes",200706),gte("Código_mes",200701))));

        //El segundo paso es sobre el conjunto anterior calcular la suma de "total_Dtes_Empleo" por provincia y se almacena en la columna "Total_Demandantes"
        query.add(Aggregates.group("$Provincia", Accumulators.sum("Total_Demandantes","$total_Dtes_Empleo")));

        //Se selecciona aquellos registros con un numero de demandantes por provincia mayor o igual a 1000000
        query.add(Aggregates.match(gte("Total_Demandantes",1000000)));

        //Se seleccionan que columnas se quieren visualizar. Se añade una columna calculada denominada "Doble"
        query.add(Aggregates.project(Projections.fields(Projections.excludeId(),Projections.include("Total_Demandantes"),Projections.computed("Doble",multiply))));

        //Se ordena el resultado anterior por el Total de demandantes en orden descendente
        query.add(Aggregates.sort(Sorts.descending("Total_Demandantes")));


        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado
        MongoCursor<Document> cursor = coll.aggregate(query).iterator();

        //TODO Mostrar el resultado
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }

        //TODO Cerrar la conexión con la base de datos
        mongo.close();

    }
}
