package org.akhq.models.decorators;

import io.micronaut.core.util.StringUtils;

public class RegexObfuscationSearchStrategy implements ObfuscationSearchStrategy {
    private final String regexPattern;

    public RegexObfuscationSearchStrategy(String regexPattern) {
        this.regexPattern = regexPattern;
    }

    @Override
    public String obfuscate(String raw, ObfuscationReplaceStrategy replaceStrategy) {
        if (StringUtils.hasText(raw)) {
            return raw.replaceAll(regexPattern, replaceStrategy.replace(raw));
        }
        return raw;
    }
}
