package org.gbif.occurrence.processor.parsing;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.occurrence.model.RawOccurrenceRecord;
import org.gbif.occurrence.parsing.xml.XmlFragmentParser;
import org.gbif.occurrence.persistence.api.Fragment;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * A thin wrapper around the XmlFragmentParser from the occurrence-parser project, and the local JsonFragmentParser, in
 * order to produce VerbatimOccurrence objects from Fragments.
 */
public class FragmentParser {

  private static final Timer xmlParsingTimer =
    Metrics.newTimer(FragmentParser.class, "verb xml parse time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private static final Timer jsonParsingTimer =
    Metrics.newTimer(FragmentParser.class, "verb json parse time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  // should not be instantiated
  private FragmentParser() {
  }

  /**
   * Parse the given fragment into a VerbatimOccurrence object.
   * TODO: there is a lot of hacking here that should be refactored out with general rework of parsing project
   *
   * @param fragment containing parsing to be parsed
   *
   * @return a VerbatimOccurrence or null if the fragment could not be parsed
   */
  @Nullable
  public static VerbatimOccurrence parse(Fragment fragment) {
    VerbatimOccurrence verbatim = null;

    switch (fragment.getFragmentType()) {
      case XML:
        final TimerContext xmlContext = xmlParsingTimer.time();
        try {
          RawOccurrenceRecord ror =
            XmlFragmentParser.parseRecord(fragment.getData(), fragment.getXmlSchema(), fragment.getUnitQualifier());
          verbatim = buildVerbatim(ror, fragment);
        } finally {
          xmlContext.stop();
        }
        break;
      case JSON:
        final TimerContext jsonContext = jsonParsingTimer.time();
        try {
          verbatim = JsonFragmentParser.parseRecord(fragment);
        } finally {
          jsonContext.stop();
        }
        break;
    }

    if (verbatim != null) {
      verbatim.setProtocol(fragment.getProtocol());
    }

    return verbatim;
  }

  // TODO: implement if really needed!!!
  private static VerbatimOccurrence buildVerbatim(RawOccurrenceRecord ror, Fragment frag) {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setKey(frag.getKey());
    v.setDatasetKey(frag.getDatasetKey());
    return v;
  }
}
