package io.apicurio.registry.storage.impl;

import java.util.concurrent.atomic.AtomicLong;
import javax.enterprise.context.ApplicationScoped;

/**
 * @author Ales Justin
 */
@ApplicationScoped
public class InMemoryRegistryStorage extends SimpleMapRegistryStorage {

    private AtomicLong counter = new AtomicLong(1);

    @Override
    protected long nextGlobalId() {
        return counter.getAndIncrement();
    }
}
