package org.akhq.models.decorators;

import com.jayway.jsonpath.JsonPath;
import io.micronaut.core.util.StringUtils;

public class JsonPathObfuscationSearchStrategy implements ObfuscationSearchStrategy {
    private final String jsonPath;

    public JsonPathObfuscationSearchStrategy(String jsonPath) {
        this.jsonPath = jsonPath;
    }

    @Override
    public String obfuscate(String raw, ObfuscationReplaceStrategy replaceStrategy) {
        if (StringUtils.hasText(raw)) {
            return JsonPath.parse(raw).set(jsonPath, replaceStrategy.replace(raw)).toString();
        }
        return raw;
    }
}
