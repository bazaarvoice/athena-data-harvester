package com.bazaarvoice.nn.nataraja.athenadataharvester.util;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

public class ListPagingIterator<I, T> implements Iterator<List<T>> {

    private Function<T, I> _idExtractor;
    private Function<Optional<I>, List<T>> _pageGetter;

    private List<T> _currentPage = null;

    public ListPagingIterator(Function<T, I> idExtractor, Function<Optional<I>, List<T>> pageGetter) {
        this._idExtractor = idExtractor;
        this._pageGetter = pageGetter;
    }

    @Override
    public boolean hasNext() {
        return _currentPage == null || !_currentPage.isEmpty();
    }

    @Override
    public List<T> next() {
        if (_currentPage != null && _currentPage.isEmpty()) {
            throw new NoSuchElementException("no further pages available");
        }


        final Optional<I> nextId;
        if (_currentPage == null) {
            nextId = Optional.empty();
        } else {
            final T lastItem = _currentPage.get(_currentPage.size() - 1);
            nextId = Optional.of(_idExtractor.apply(lastItem));
        }

        _currentPage = _pageGetter.apply(nextId);
        return _currentPage;
    }
}