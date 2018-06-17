package net.marco27.apps.book;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import net.marco27.apps.book.domain.Book;
import net.marco27.apps.book.repository.BookRepository;
import net.marco27.apps.book.repository.KeyspaceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraClient {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);

    public static void main(String args[]) {
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", null);
        Session session = connector.getSession();

        KeyspaceRepository keyspaceRepository = new KeyspaceRepository(session);
        keyspaceRepository.createKeyspace("library", "SimpleStrategy", 1);
        keyspaceRepository.useKeyspace("library");

        BookRepository bookRepository = new BookRepository(session);
        bookRepository.createTable();
        bookRepository.alterTablebooks("publisher", "text");

        bookRepository.createTableBooksByTitle();

        Book book = new Book(UUIDs.timeBased(), "Effective Java", "Joshua Bloch", "Programming");
        bookRepository.insertBookBatch(book);

        bookRepository.selectAll().forEach(o -> LOG.info("Title in books: " + o.getTitle()));
        bookRepository.selectAllBookByTitle().forEach(o -> LOG.info("Title in booksByTitle: " + o.getTitle()));

        bookRepository.deletebookByTitle("Effective Java");
        bookRepository.deleteTable("books");
        bookRepository.deleteTable("booksByTitle");

        keyspaceRepository.deleteKeyspace("library");

        connector.close();
    }
}