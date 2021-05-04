package org.akhq.models.decorators;

import lombok.extern.slf4j.Slf4j;
import org.akhq.models.Record;

@Slf4j
public class ObfuscatedRecord extends RecordDecorator {
    private final ObfuscationSearchStrategy searchStrategy;
    private final ObfuscationReplaceStrategy replaceStrategy;

    public ObfuscatedRecord(Record record, ObfuscationSearchStrategy searchStrategy, ObfuscationReplaceStrategy replaceStrategy) {
        super(record);
        this.searchStrategy = searchStrategy;
        this.replaceStrategy = replaceStrategy;
    }

    @Override
    public String getValue() {
        return searchStrategy.obfuscate(super.getValue(), replaceStrategy);
    }
}
