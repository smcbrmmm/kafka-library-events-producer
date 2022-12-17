package com.example.libraryeventproducer.Domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class LibraryEvent {

    private int libraryEventId;
    private Book book;
}
