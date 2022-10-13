package com.bazaarvoice.nn.nataraja.athenadataharvester.util;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class ListPager<I, T> implements Iterable<List<T>> {

    private Function<T, I> _idExtractor;
    private Function<Optional<I>, List<T>> _pageGetter;

    public ListPager(Function<T, I> idExtractor, Function<Optional<I>, List<T>> pageGetter) {
        this._idExtractor = idExtractor;
        this._pageGetter = pageGetter;
    }


    @Override
    public Iterator<List<T>> iterator() {
        return new ListPagingIterator<>(_idExtractor, _pageGetter);
    }
}
