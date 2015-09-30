package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.MediaParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

import java.net.URI;
import java.util.Date;
import java.util.Map;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interprets multi media extension records.
 */
public class MultiMediaInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(MultiMediaInterpreter.class);
  private static final MediaParser mediaParser = MediaParser.getInstance();

  private MultiMediaInterpreter() {
  }

  public static void interpretMedia(VerbatimOccurrence verbatim, Occurrence occ) {
    // media via core term
    if (verbatim.hasVerbatimField(DwcTerm.associatedMedia)) {
      for (URI uri : UrlParser.parseUriList(verbatim.getVerbatimField(DwcTerm.associatedMedia))) {
        if (uri == null) {
          occ.getIssues().add(OccurrenceIssue.MULTIMEDIA_URI_INVALID);

        } else {
          MediaObject m = new MediaObject();
          m.setIdentifier(uri);
          mediaParser.detectType(m);
          occ.getMedia().add(m);
        }
      }
    }

    // simple image or multimedia extension which are nearly identical
    if (verbatim.getExtensions().containsKey(Extension.IMAGE) || verbatim.getExtensions().containsKey(Extension.MULTIMEDIA)) {
      final Extension mediaExt = verbatim.getExtensions().containsKey(Extension.IMAGE) ? Extension.IMAGE : Extension.MULTIMEDIA;
      for (Map<Term, String> rec : verbatim.getExtensions().get(mediaExt)) {
        URI uri = UrlParser.parse(rec.get(DcTerm.identifier));
        URI link = UrlParser.parse(rec.get(DcTerm.references));
        // link or media uri must exist
        if (uri != null || link != null) {
          MediaObject m = new MediaObject();
          m.setIdentifier(uri);
          m.setReferences(link);
          m.setTitle(rec.get(DcTerm.title));
          m.setDescription(rec.get(DcTerm.description));
          m.setLicense(rec.get(DcTerm.license));
          m.setPublisher(rec.get(DcTerm.publisher));
          m.setContributor(rec.get(DcTerm.contributor));
          m.setSource(rec.get(DcTerm.source));
          m.setAudience(rec.get(DcTerm.audience));
          m.setRightsHolder(rec.get(DcTerm.rightsHolder));
          m.setCreator(rec.get(DcTerm.creator));
          m.setFormat(mediaParser.parseMimeType(rec.get(DcTerm.format)));
          if (rec.containsKey(DcTerm.created)) {
            Range<Date> validRecordedDateRange = Range.closed(TemporalInterpreter.MIN_VALID_RECORDED_DATE, new Date());
            OccurrenceParseResult<Date> parsed = TemporalInterpreter.interpretDate(rec.get(DcTerm.created),
                                          validRecordedDateRange, OccurrenceIssue.MULTIMEDIA_DATE_INVALID);
            m.setCreated(parsed.getPayload());
            occ.getIssues().addAll(parsed.getIssues());
          }

          mediaParser.detectType(m);
          occ.getMedia().add(m);

        } else {
          occ.getIssues().add(OccurrenceIssue.MULTIMEDIA_URI_INVALID);
        }
      }
    }

    // merge information if the same image URL is given several times, e.g. via the core AND an extension
    deduplicateMedia(occ);
  }

  /**
   * merges media records if the same image URL or link is given several times.
   * Remove any media that has not either a file or webpage uri.
   */
  private static void deduplicateMedia(Occurrence occ) {
    Map<String, MediaObject> media = Maps.newLinkedHashMap();
    for (MediaObject m : occ.getMedia()) {
      // we can get file uris or weblinks. Prefer file URIs as they clearly identify a single image
      URI uri = m.getIdentifier() != null ? m.getIdentifier() : m.getReferences();
      if (uri != null) {
        String url = uri.toString();
        if (media.containsKey(url)) {
          // merge infos about the same image?
        } else {
          media.put(url, m);
        }
      }
    }
    occ.setMedia(Lists.newArrayList(media.values()));
  }

}
