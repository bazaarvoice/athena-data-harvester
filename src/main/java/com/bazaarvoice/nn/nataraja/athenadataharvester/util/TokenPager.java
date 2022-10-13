package com.bazaarvoice.nn.nataraja.athenadataharvester.util;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@RequiredArgsConstructor
@Accessors(prefix = "_")
public class TokenPager<I, T> implements Iterable<List<T>> {

    private final Function<Optional<I>, TokenPagedData> _dataGetter; // this takes in an Optional token and returns a TokenPagedData

    @Override
    public Iterator<List<T>> iterator() {
        return new TokenPagingIterator<>(_dataGetter);
    }
}
