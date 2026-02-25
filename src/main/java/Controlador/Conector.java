package Controlador;

import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import javax.swing.JOptionPane;
import org.bson.Document;
import org.bson.types.ObjectId;

public class Conector {

    private final String db = "paises"; //nombre de la base de datos en mongo
    private final String url = "mongodb://localhost:27017";
    private final String urlatlas = "";

    MongoDatabase mdb;

    public Conector() {
    }

    //nos conectamos a la base de datos
    public boolean conectar() {
        boolean b = false;

        MongoClient mongo = MongoClients.create(url);
        if (mongo != null) {
            mdb = mongo.getDatabase(db);
            b = true;
        } else {
            JOptionPane.showMessageDialog(null, "Error de conexion con MongoDB", "Error", JOptionPane.ERROR_MESSAGE);
        }
        return b;
    }

    //método para agregar un nuevo continente 
    public boolean altaContinente(String continentev) {
        boolean b = false;
        if (continentev != null) {
            MongoCollection<Document> coleccion = mdb.getCollection("continentes");
            try {
                Document continente = new Document("nombre", continentev);
                coleccion.insertOne(continente);
                b = true;
            } catch (MongoWriteException e) {
                if (e.getCode() == 11000) {
                    JOptionPane.showMessageDialog(null, "ERROR. El continente ya existe.");
                } else {
                    JOptionPane.showMessageDialog(null, "Error de escritura en MongoDB.");
                }
            }

        } else {
            System.err.println("Nombre sin valor");
        }

        return b;
    }

    public boolean altaPais(String continentev, int numHabitantesv, String nombrev) {
        boolean b = false;
        if (nombrev != null) {
            MongoCollection<Document> coleccion = mdb.getCollection("continentes");
            MongoCollection<Document> paises = mdb.getCollection("paises");

            Document continente = coleccion.find(new Document("nombre", continentev)).first();

            ObjectId continenteId = continente.getObjectId("_id");

            try {
                Document pais = new Document("habitantes", numHabitantesv)
                        .append("nombre", nombrev)
                        .append("continenteId", continenteId);
                paises.insertOne(pais);
                b = true;
            } catch (MongoWriteException e) {
                if (e.getCode() == 11000) {
                    System.err.println("Error: Duplicidad en al clave");
                } else {
                    System.err.println("Error de escritura en Mongo");
                }
            }

        } else {
            System.err.println("Nombre sin valor");
        }

        return b;
    }

    public boolean EliminarPais(String nombre) {
        boolean b = false;

        MongoCollection<Document> col = mdb.getCollection("paises");

        Document doc = new Document("nombre", nombre);

        if (doc != null) {
            col.findOneAndDelete(doc);
            b = true;
        }
        return b;
    }

    public ArrayList<Document> listarPaises() {
        ArrayList<Document> lista = new ArrayList<>();

        MongoCollection<Document> coleccion = mdb.getCollection("paises");

        FindIterable<Document> documentos = coleccion.find();

        for (Document doc : documentos) {
            lista.add(doc);
        }

        return lista;
    }

    public ArrayList<Document> listarContinentes() {
        ArrayList<Document> lista = new ArrayList<>();

        MongoCollection<Document> coleccion = mdb.getCollection("continentes");

        FindIterable<Document> documentos = coleccion.find();

        for (Document doc : documentos) {
            lista.add(doc);
        }

        return lista;
    }

    // método que devuelve una lista de países por el id del continente 
    public ArrayList<Document> listarId(String continentev) {
        ArrayList<Document> lista = new ArrayList<>();
        MongoCollection<Document> coleccion = mdb.getCollection("continentes");
        MongoCollection<Document> paises = mdb.getCollection("paises");
        Document continente = coleccion.find(new Document("nombre", continentev)).first();
        ObjectId continenteId = continente.getObjectId("_id"); //obtiene el id del continente 
        
        //Busca en la colección de países todos los documentos donde el campo continenteId es igual al id del continente
        FindIterable<Document> documentos = paises.find(new Document("continenteId", continenteId));
        
        //recorre los paises encontrados y los añade a la lista
        for (Document documento : documentos) {
            lista.add(documento);
        }
        return lista;
    }
}
