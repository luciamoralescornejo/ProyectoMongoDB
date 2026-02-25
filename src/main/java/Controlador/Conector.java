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

/**
 * Clase encargada de gestionar la conexión y las operaciones CRUD con la base
 * de datos MongoDB para la gestión de continentes y países.
 */
public class Conector {

    private final String db = "paises";
    private final String url = "mongodb://localhost:27017";
    private final String urlatlas = "";

    MongoDatabase mdb;

    public Conector() {
    }

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
    
    public boolean EliminarPais (String nombre) {
        boolean b = false; 
        
        MongoCollection<Document> col = mdb.getCollection("paises");
        
        Document doc = new Document("nombre", nombre); 
        
        if(doc != null) {
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
}