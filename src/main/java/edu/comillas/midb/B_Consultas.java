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

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class B_Consultas {
    public static void main (String[] args){
        //Crear un objeto cliente de mongo
        MongoClient mongo = new MongoClient("127.0.0.1", 27017);

        //Obtener la base de datos "Empleo"
        //Se obtiene la base de datos con la que se va a trabajar. En este caso "Empleo"
        MongoDatabase db1 = mongo.getDatabase("Empleo");

        //Obtener la coleccion "Solicitudes"
        String collectionName = "Solicitudes";
        //Se obtiene la colleccion de documentos
        MongoCollection<Document> coll = db1.getCollection(collectionName);


        //////////////////////////////////////////////
        //CONSULTA 1//////////////////////////////////
        /////////////////////////////////////////////

        Document query = new Document("$and", Arrays.asList(
                new Document("mes", "Enero de 2007"),
                new Document("$or", Arrays.asList(
                        new Document("$and", Arrays.asList(
                                new Document("Provincia","Barcelona"),
                                new Document("Dtes_Empleo_mujer_edad_>=45", new Document("$gte",1000))
                        )),
                        new Document("$and", Arrays.asList(
                                new Document("Provincia","Bizkaia"),
                                new Document("Dtes_Empleo_mujer_edad_>=45", new Document("$gte",7).append("$lte",10))
                        ))
                ))
        ));


        MongoCursor<Document> cursor = coll.find(query)
                .projection(fields(include("Municipio","Provincia","Dtes_Empleo_mujer_edad_>=45"),excludeId()))
                .iterator();

        //ENUNCIADO
        System.out.println("\n");
        System.out.println("1. Obtener un listado compuesto por Provincia, Municipio y Demandantes de empleo mujer de edad  mayor o igual a 45 de enero de 2007 con las siguientes consideraciones:");
        System.out.println("- Si es de Barcelona se mostrarán aquellas que el número de demandantes sea igual o superior a 1000");
        System.out.println("- Si es de Bizkaia el número de demandantes debe estar entre 10 y  7 ambos incluidos.");
        System.out.println("\n");

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }


        //Se imprimen en la consola dos lineas en blanco
        System.out.println("\n\n");



        //////////////////////////////////////////////
        //CONSULTA 2//////////////////////////////////
        /////////////////////////////////////////////

        //Se crea una lista donde ir almacenando los diferentes pasos del proceso
        List<Bson> query2 = new ArrayList<Bson>();

        //Se calcular la suma de "total_Dtes_Empleo" por provincia y se almacena en la columna "Total_Demandantes"
        query2.add(Aggregates.group("$Provincia", Accumulators.sum("Total_Demandantes","$total_Dtes_Empleo")));

        //Se selecciona aquellos registros con un numero de demandantes por provincia mayor o igual a 1700000
        query2.add(Aggregates.match(gte("Total_Demandantes",1700000)));

        //Se ordena el resultado anterior por el Total de demandantes en orden descendente
        query2.add(Aggregates.sort(Sorts.descending("Total_Demandantes")));

        //Se seleccionan que columnas se quieren visualizar. Se añade una columna calculada denominada "Doble"
        query2.add(Aggregates.project(Projections.fields(Projections.include("Provincia"))));

        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado
        cursor = coll.aggregate(query2).iterator();

        //ENUNCIADO
        System.out.println("\n");
        System.out.println("2. Obtener una lista ordenada de mayor a menor de Provincias según el número total de demandantes de empleo según ");
        System.out.println("la información contenida en la base de datos. Mostrar solo aquellas con numero total de demandantes mayor de 1700000.");
        System.out.println("\n");


        //TMostrar el resultado
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }

        //Se imprimen en la consola dos lineas en blanco
        System.out.println("\n\n");

        //////////////////////////////////////////////
        //CONSULTA 3//////////////////////////////////
        /////////////////////////////////////////////

        //Se crea un documento con un Json para crear un campo que calcule el porcentaje de Demandantes de Empleo en el Sector Servicios
        Document percent = new Document("$multiply", Arrays.asList(new Document("$divide", Arrays.asList("$Municipio.Dtes_Empleo_Servicios","$sum")), 100));

        //Se crea una lista donde ir almacenando los diferentes pasos del proceso
        List<Bson> query3 = new ArrayList<Bson>();

        //Filtramos solo HUevla en Enero de 2007
        query3.add(Aggregates.match(eq("Provincia","Huelva")));
        query3.add(Aggregates.match(eq("mes","Enero de 2007")));

        //Se calcular la suma de "Dtes_Empleo_Servicios" por provincia y se almacena en la columna "suma"
        query3.add(Aggregates.group("$Provincia", Accumulators.sum("sum","$Dtes_Empleo_Servicios"),
                Accumulators.push("Municipio", new Document("Municipio", "$Municipio").append("Dtes_Empleo_Servicios", "$Dtes_Empleo_Servicios"))));

        query3.add(Aggregates.unwind("$Municipio"));

        //Se seleccionan que columnas se quieren visualizar. Se añade una columna calculada denominada "Porcentaje"
        query3.add(Aggregates.project(Projections.fields(Projections.excludeId(), Projections.include("Municipio.Municipio"),Projections.computed("Porcentaje",percent))));

        //Se ordena el resultado anterior por el Porcentaje en orden descendente
        query3.add(Aggregates.sort(Sorts.descending("Porcentaje")));

        //Se muestra solo el primer resultado
        query3.add(Aggregates.limit(1));

        //Se ejecuta el pipile deninido anteriormente y se obtiene un cursor con el resultado
        cursor = coll.aggregate(query3).iterator();

        //ENUNCIADO
        System.out.println("\n");
        System.out.println("3. Mostrar sólo el nombre del municipio de Huelva con mayor porcentaje de");
        System.out.println("solicitudes del sector servicios del mes de marzo del 2007.");
        System.out.println("\n");


        //TMostrar el resultado
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }

        //Se imprimen en la consola dos lineas en blanco
        System.out.println("\n\n");



        //Cerrar la conexión con la base de datos
        mongo.close();

    }
}
