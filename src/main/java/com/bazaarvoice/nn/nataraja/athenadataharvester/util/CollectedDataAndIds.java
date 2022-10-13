package com.bazaarvoice.nn.nataraja.athenadataharvester.util;

import com.bazaarvoice.memento.client.model.Identifiers;
import com.google.common.base.Preconditions;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class CollectedDataAndIds {
    private final Collection<Path> collectedData;

    private final Collection<Identifiers> discoveredIds;

    private final Optional<String> comment;

    public CollectedDataAndIds(Collection<Path> collectedData, Collection<Identifiers> discoveredIds) {
        this(collectedData, discoveredIds, Optional.empty());
    }

    public CollectedDataAndIds(Collection<Path> collectedData, Collection<Identifiers> discoveredIds, Optional<String> comment) {
        this.collectedData = collectedData;
        this.discoveredIds = discoveredIds;
        this.comment = comment;
    }

    public static CollectedDataAndIds dataOnly(Collection<Path> data) {
        return new CollectedDataAndIds(data, Collections.emptyList(), Optional.empty());
    }

    public static CollectedDataAndIds empty() {
        return new CollectedDataAndIds(Collections.emptyList(), Collections.emptyList(), Optional.empty());
    }

    public CollectedDataAndIds withComment(String comment) {
        Preconditions.checkState(!this.comment.isPresent(), "Cannot override existing comment: %s, with %s", this.comment, comment);
        return new CollectedDataAndIds(collectedData, discoveredIds, Optional.of(comment));
    }

    public boolean isEmpty() {
        return collectedData.isEmpty() && discoveredIds.isEmpty();
    }

    public Collection<Path> getCollectedData() {
        return collectedData;
    }

    public Collection<Identifiers> getDiscoveredIds() {
        return discoveredIds;
    }

    public Optional<String> getComment() {
        return comment;
    }
}
