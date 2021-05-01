package org.akhq.models.decorators;

import org.akhq.models.Record;

public interface ObfuscationSearchStrategy {
    String obfuscate(String raw, ObfuscationReplaceStrategy replaceStrategy);
}
