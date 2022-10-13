package com.bazaarvoice.nn.nataraja.athenadataharvester.util;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@RequiredArgsConstructor
@Accessors (prefix = "_")
public class TokenPagingIterator<I, T> implements Iterator<List<T>> {

    private final Function<Optional<I>, TokenPagedData> _pageGetter;

    private TokenPagedData<I, T> _currentPage;

    @Override
    public boolean hasNext() {
        return _currentPage == null || _currentPage.getNextToken() != null;
    }

    @Override
    public List<T> next() {
        final Optional<I> nextId;
        if (_currentPage == null) {
            nextId = Optional.empty();
        } else {
            nextId = Optional.of(_currentPage.getNextToken());
        }

        _currentPage = _pageGetter.apply(nextId);
        return _currentPage.getData();
    }
}
