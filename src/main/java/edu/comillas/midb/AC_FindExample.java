package edu.comillas.midb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.util.Arrays;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class AC_FindExample {
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

        //Se obtiene el primer documento de la coleccion
        Document myDoc = (Document) coll.find().first();
        //Extraer el mes de la solicitud
        String mes = myDoc.getString("mes");
        //Se muestra el Json del documento
        System.out.println(myDoc.toJson());
        //Se obtiene el número de documentos que componen la coleccion
        long elementos = coll.countDocuments();
        //Se imprime en la consola el numero de elementos
        System.out.println("El numero docuemntos es " + elementos);




        //Se obtiene un iterador para poder recorrer la coleccion
        MongoCursor<Document> cursor = coll.find().iterator();
        try {
            int i = 0;
            //Se hace un bucle para imprimir en la consola los documentos de la coleccion
            while (cursor.hasNext() && i<100) { //sacamos los 100 primeros elementos del cursor
                System.out.println(cursor.next().toJson());
                i++;

            }
        } finally {
            //Se cierra el cursor
            cursor.close();
        }


        //Se imprimen en la consola dos lineas en blanco
        System.out.println("\n\n");


        //Obtener las "Provincia","Comunidad_Autónoma" y"mes" de las solicitudes cuyo "Código_de_CA" se igual a 9 y
        // el "Codigo_Municipio" sea 8072, ordenadas por mes en sentido descendente del mes excluyendo el id

        Document query = new Document("Código_de_CA", 9).append("Codigo_Municipio",8072);

        cursor = coll.find(query).sort(Sorts.descending("mes")).iterator();

            //otra forma de hacerlo
        cursor = coll.find(and(eq("Código_de_CA",9),eq("Codigo_Municipio",8072))) //HAGO EL AND DE DOS IGUALDADES (EQL)
                .sort(Sorts.descending("mes"))
                .projection(fields(include("Provincia","Comunidad_Autónoma","mes"), excludeId()))
                .iterator();

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }


        //Se imprimen en la consola dos lineas en blanco
        System.out.println("\n\n");

        //Se crea un Json con la consulta que se quiere ejecutar como si se estuviese en el shell
        //Obtener las solicitudes donde el "Codigo de CA" sea mayor o igual 9 y menor o igual que 11 del mes de mayo de 2007
        query = new Document("Código_de_CA", new Document("$gte",9).append("$lte",11)).append("Código_mes",200705);
        System.out.println(query.toJson());

        //Se obtiene un iterador sobre los resultados de la consulta
        cursor = coll.find(query).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }

        //Se imprimen en la consola dos lineas en blanco
        System.out.println("\n\n");

        //Se hace la misma consulta directamente sobre el metodo find
        cursor = coll.find(and(gte("Código_de_CA",9),lt("Código_de_CA",11),eq("Código_mes",200705))).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }




        //Se define una query como un documento Json.
        //Obtener las entradas de los municipios de Colmenar Viejo, Alcobendas y Madrid entre los meses de enero y marzo  del 2007 ambos inclusive
        //cuando los demandantes de empleo sin empleo anterior * 100 sea menor que 10000
        query = new Document("$and", Arrays.asList(
                new Document("Municipio", new Document("$in", Arrays.asList("Colmenar Viejo", "Alcobendas", "Madrid"))),
                new Document("Código_mes",new Document("$gte",200701).append("$lte",200703)),
                new Document("$where","(this.Dtes_Empleo_Sin_empleo_Anterior * 100 )< 10000")
        ));

        //Se imprime el documento en formato Json
        System.out.println(query.toJson());

        cursor = coll.find(query)
                .projection(fields(include("Municipio","Comunidad Autónoma","mes","Dtes_Empleo_Sin_empleo_Anterior"),excludeId()))
                .iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }


        //TODO cerrar la conexión con la base de datos
        mongo.close();


    }
}
