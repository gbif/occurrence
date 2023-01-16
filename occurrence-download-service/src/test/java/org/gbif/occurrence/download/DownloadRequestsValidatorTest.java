package org.gbif.occurrence.download;

import org.gbif.occurrence.download.service.DownloadRequestsValidator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DownloadRequestsValidatorTest {

  @Test
  public void downloadTestSendNotificationNull() {
    DownloadRequestsValidator validator = new DownloadRequestsValidator();
    validator.validate("{\n"
                       + "  \"creator\":\"markus\",\n"
                       + "  \"format\": \"SIMPLE_CSV\",\n"
                       + "  \"predicate\":{\n"
                       + "    \"type\":\"within\",\n"
                       + "    \"geometry\":\"POLYGON ((-85.781 17.978,-81.035 14.774,-77.343 10.314,-79.277 6.315,-93.955 14.604,-91.450 18.229,-87.626 19.311,-85.781 17.978))\"\n"
                       + "  }\n"
                       + "}");
  }

  @Test
  public void downloadTestWrongPredicate() {
    DownloadRequestsValidator validator = new DownloadRequestsValidator();
    Assertions.assertThrows(org.everit.json.schema.ValidationException.class, () ->
    validator.validate("{\n"
                       + "  \"creator\":\"markus\",\n"
                       + "  \"format\": \"SIMPLE_CSV\",\n"
                       + "  \"predicate\":{\n"
                       + "    \"type\":\"inner\",\n" //Wrong predicate name
                       + "    \"geometry\":\"POLYGON ((-85.781 17.978,-81.035 14.774,-77.343 10.314,-79.277 6.315,-93.955 14.604,-91.450 18.229,-87.626 19.311,-85.781 17.978))\"\n"
                       + "  }\n"
                       + "}"));
  }


  @Test
  public void downloadTestUnknownField() {
    DownloadRequestsValidator validator = new DownloadRequestsValidator();
    Assertions.assertThrows(org.everit.json.schema.ValidationException.class, () ->
      validator.validate("{\n"
                         + "  \"creator\":\"markus\",\n"
                         + "  \"format_type\": \"SIMPLE_CSV\",\n" //Unknown field
                         + "  \"predicate\":{\n"
                         + "    \"type\":\"inner\",\n" //Wrong predicate name
                         + "    \"geometry\":\"POLYGON ((-85.781 17.978,-81.035 14.774,-77.343 10.314,-79.277 6.315,-93.955 14.604,-91.450 18.229,-87.626 19.311,-85.781 17.978))\"\n"
                         + "  }\n"
                         + "}"));
  }
}
