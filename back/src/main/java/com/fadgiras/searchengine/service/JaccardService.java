package com.fadgiras.searchengine.service;

import com.fadgiras.searchengine.dto.BookCardDTO;
import com.fadgiras.searchengine.model.Book;
import com.fadgiras.searchengine.model.JaccardBook;
import com.fadgiras.searchengine.repository.BookRepository;
import com.fadgiras.searchengine.repository.JaccardBookRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class JaccardService {

    @Autowired
    BookRepository bookRepository;

    @Autowired
    JaccardBookRepository jaccardBookRepository;

    private static final Logger logger = LoggerFactory.getLogger(JaccardService.class);

    public String calculateJaccardDistances() {
        jaccardBookRepository.deleteAll(); // Supprime toutes les distances de Jaccard de la base de données
        List<Book> books = bookRepository.findAll(); // Récupère tous les livres de la base de données

        record BookAndWords(Book book, Set<String> words) {}
        // Map pour stocker les ensembles de mots pour chaque livre
        List<BookAndWords> allBookAndWords = books.stream()
                .map(book -> new BookAndWords(book, getWords(book)))
                .toList();

        record BookCouple(BookAndWords bookAndWords, BookAndWords otherBookAndWords) {}
        Set<BookCouple> bookCouples = allBookAndWords.stream().flatMap(bookAndWords ->
                        allBookAndWords.stream()
                                .filter(otherBookAndWords -> bookAndWords.book() != otherBookAndWords.book())
                                .map(otherBookAndWords -> new BookCouple(bookAndWords, otherBookAndWords)))
                .collect(Collectors.toSet());

        // Calcul de la distance de Jaccard entre chaque paire de livres
        List<JaccardBook> jaccardBooks = bookCouples.parallelStream().map(bookCouple -> {
            logger.trace("{} / {}",
                    bookCouple.bookAndWords().book().getTitle(),
                    bookCouple.otherBookAndWords().book().getTitle());

            double jaccardIndex = calculateJaccardIndex(
                    bookCouple.bookAndWords().words(),
                    bookCouple.otherBookAndWords().words());
            double jaccardDistance = 1 - jaccardIndex;

            return new JaccardBook(
                    bookCouple.bookAndWords().book(),
                    bookCouple.otherBookAndWords().book(),
                    jaccardDistance);
        }).toList();

        // Enregistrement des distances de Jaccard dans la base de données
         jaccardBookRepository.saveAll(jaccardBooks);

        return "OK";
    }

    private Set<String> getWords(Book book) {
        return Arrays.stream(book.getContent().toLowerCase().split("\\W+"))
                .parallel()
                .collect(Collectors.toSet());
    }

    private double calculateJaccardIndex(Set<String> set1, Set<String> set2) {
        Set<String> intersection = new HashSet<>(set1);
        intersection.retainAll(set2);

        Set<String> union = new HashSet<>(set1);
        union.addAll(set2);

        return intersection.size() / (double) union.size();
    }

    public List<BookCardDTO> getSuggestedBooks(int bookId, List<BookCardDTO> foundBooks) {
        Book book = bookRepository.findById(bookId).orElse(null);
        if (book == null) {
            return Collections.emptyList();
        }

        List<Object[]> suggestedBookIdsAndDistances = jaccardBookRepository.getSuggestedBookIdsAndDistances(book.getId());
        List<BookCardDTO> suggestedBooks = new ArrayList<>();

        int limit = 3;
        for (Object[] suggestedBookIdAndDistance : suggestedBookIdsAndDistances) {
            Long suggestedBookId = (Long) suggestedBookIdAndDistance[0];
            Book suggestedBook = bookRepository.findById(Math.toIntExact(suggestedBookId)).orElse(null);

            if (foundBooks.stream().anyMatch(b -> b.getId().equals(suggestedBookId))) continue;

            if (suggestedBook != null && suggestedBooks.size() < limit) {
                suggestedBooks.add(new BookCardDTO(suggestedBook));
            }else {
                break;
            }
        }

        return suggestedBooks;
    }
}
