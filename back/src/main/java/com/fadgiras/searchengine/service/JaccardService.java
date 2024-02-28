package com.fadgiras.searchengine.service;

import com.fadgiras.searchengine.controller.MainController;
import com.fadgiras.searchengine.dto.BookCardDTO;
import com.fadgiras.searchengine.model.Book;
import com.fadgiras.searchengine.model.JaccardBook;
import com.fadgiras.searchengine.repository.BookRepository;
import com.fadgiras.searchengine.repository.JaccardBookRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class JaccardService {

    @Autowired
    BookRepository bookRepository;

    @Autowired
    JaccardBookRepository jaccardBookRepository;

    private static final Logger logger = LoggerFactory.getLogger(JaccardService.class);

    private record BookCouple(Book book, Book other) {}

    public String calculateJaccardDistances() {
        jaccardBookRepository.deleteAll(); // Supprime toutes les distances de Jaccard de la base de données
        List<Book> books = bookRepository.findAll(); // Récupère tous les livres de la base de données

        // Map pour stocker les ensembles de mots pour chaque livre
        Map<Book, Set<String>> bookWords = books.parallelStream()
                .collect(Collectors.toMap(Function.identity(), this::getWords));

        Set<BookCouple> bookCouples = books.stream().flatMap(book ->
                        books.stream()
                                .filter(otherBook -> book != otherBook)
                                .map(otherBook -> new BookCouple(book, otherBook)))
                .collect(Collectors.toSet());

        // Calcul de la distance de Jaccard entre chaque paire de livres
        List<JaccardBook> jaccardBooks = bookCouples.parallelStream()
                .map(bookCouple -> computeJaccardBook(bookCouple, bookWords))
                .toList();

        // Enregistrement des distances de Jaccard dans la base de données
        jaccardBookRepository.saveAll(jaccardBooks);

        return "OK";
    }

    private JaccardBook computeJaccardBook(BookCouple bookCouple, Map<Book, Set<String>> bookWords) {
        logger.trace("{} / {}", bookCouple.book().getTitle(), bookCouple.other().getTitle());

        Set<String> words1 = bookWords.get(bookCouple.book());
        Set<String> words2 = bookWords.get(bookCouple.other());

        double jaccardIndex = calculateJaccardIndex(words1, words2);
        double jaccardDistance = 1 - jaccardIndex;

        return new JaccardBook(bookCouple.book(), bookCouple.other(), jaccardDistance);
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
