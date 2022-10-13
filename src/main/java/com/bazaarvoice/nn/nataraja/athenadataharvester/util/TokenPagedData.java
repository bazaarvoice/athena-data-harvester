package com.bazaarvoice.nn.nataraja.athenadataharvester.util;

import lombok.Getter;

import java.util.List;

@Getter
public class TokenPagedData <I, T> {

    private List<T> data;
    private I nextToken;

    public TokenPagedData(List<T> data, I nextToken) {
        this.data = data;
        this.nextToken = nextToken;
    }
}
