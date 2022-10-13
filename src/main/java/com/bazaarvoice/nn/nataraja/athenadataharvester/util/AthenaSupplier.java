package com.bazaarvoice.nn.nataraja.athenadataharvester.util;

import com.amazonaws.services.athena.AmazonAthena;

import java.util.function.Supplier;

/**
 * exists to simplify injection
 */
public interface AthenaSupplier extends Supplier<AmazonAthena> {
    static AthenaSupplier wrap(Supplier<AmazonAthena> supplier) {
        return supplier::get;
    }
}