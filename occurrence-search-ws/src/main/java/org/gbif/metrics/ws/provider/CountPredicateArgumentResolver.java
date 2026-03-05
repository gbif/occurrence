/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.metrics.ws.provider;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.predicate.Predicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.core.MethodParameter;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

/**
 * Resolves request query parameters into a Predicate for the occurrence count API.
 * Compatible with occurrence-search ES 9 and gbif-predicates.
 */
public class CountPredicateArgumentResolver implements HandlerMethodArgumentResolver {

  @Override
  public boolean supportsParameter(MethodParameter parameter) {
    return parameter.getParameterAnnotation(ProvidedCountPredicate.class) != null;
  }

  @Override
  public Object resolveArgument(
      MethodParameter parameter,
      ModelAndViewContainer mavContainer,
      NativeWebRequest webRequest,
      WebDataBinderFactory binderFactory) {
    Map<String, String[]> paramMap = webRequest.getParameterMap();
    List<Predicate> predicates = new ArrayList<>();
    for (Map.Entry<String, String[]> e : paramMap.entrySet()) {
      String key = e.getKey();
      String[] values = e.getValue();
      if (values == null || values.length == 0 || "callback".equalsIgnoreCase(key) || "_".equals(key)) {
        continue;
      }
      String value = values[0].trim();
      OccurrenceSearchParameter.lookup(key).ifPresent(param -> {
        if (OccurrenceSearchParameter.YEAR == param) {
          Predicate yearPredicate = parseYearPredicate(value);
          if (yearPredicate != null) {
            predicates.add(yearPredicate);
          }
        } else {
          predicates.add(new EqualsPredicate<>(param, value, false));
        }
      });
    }
    if (predicates.isEmpty()) {
      return null; // no query → total count (match all)
    }
    if (predicates.size() == 1) {
      return predicates.get(0);
    }
    return new ConjunctionPredicate(predicates);
  }

  private static Predicate parseYearPredicate(String year) {
    int now = java.util.Calendar.getInstance().get(java.util.Calendar.YEAR) + 1;
    if (year == null || year.isBlank()) {
      return new ConjunctionPredicate(List.of(
          new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.YEAR, "1500"),
          new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.YEAR, String.valueOf(now))));
    }
    String[] parts = year.split(",");
    if (parts.length == 1) {
      int from = Integer.parseInt(parts[0].trim());
      return new ConjunctionPredicate(List.of(
          new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.YEAR, String.valueOf(from)),
          new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.YEAR, String.valueOf(now))));
    }
    if (parts.length == 2) {
      int from = Integer.parseInt(parts[0].trim());
      int to = Integer.parseInt(parts[1].trim());
      return new ConjunctionPredicate(List.of(
          new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.YEAR, String.valueOf(from)),
          new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.YEAR, String.valueOf(to))));
    }
    throw new IllegalArgumentException("Invalid year range: " + year);
  }
}
