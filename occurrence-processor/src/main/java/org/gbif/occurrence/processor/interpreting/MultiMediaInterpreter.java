package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.MediaParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.dwc.terms.AcTerm;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.Terms;

import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

/**
 * Interprets multi media extension records.
 */
public class MultiMediaInterpreter {

  private static final MediaParser MEDIA_PARSER = MediaParser.getInstance();

  // Order is important in case more than one extension is provided. The order will define the precedence.
  private static final Set<Extension> SUPPORTED_MEDIA_EXTENSIONS = ImmutableSet.of(
          Extension.MULTIMEDIA, Extension.AUDUBON, Extension.IMAGE);

  /**
   * Private constructor.
   */
  private MultiMediaInterpreter() {
    //hidden constructor
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
          MEDIA_PARSER.detectType(m);
          occ.getMedia().add(m);
        }
      }
    }

    // handle possible multimedia extensions
    final Extension mediaExt = getMultimediaExtension(verbatim.getExtensions().keySet());
    if (mediaExt != null) {
      for (Map<Term, String> rec : verbatim.getExtensions().get(mediaExt)) {
        URI uri = UrlParser.parse(Terms.getValueOfFirst(rec, DcTerm.identifier, AcTerm.accessURI));
        URI link = UrlParser.parse(Terms.getValueOfFirst(rec, DcTerm.references, AcTerm.furtherInformationURL,
                AcTerm.attributionLinkURL));
        // link or media uri must exist
        if (uri != null || link != null) {
          MediaObject m = new MediaObject();
          m.setIdentifier(uri);
          m.setReferences(link);
          m.setTitle(Terms.getValueOfFirst(rec, DcTerm.title, AcTerm.caption));
          m.setDescription(rec.get(DcTerm.description));
          m.setLicense(Terms.getValueOfFirst(rec, DcTerm.license, DcTerm.rights));
          m.setPublisher(rec.get(DcTerm.publisher));
          m.setContributor(rec.get(DcTerm.contributor));
          m.setSource(Terms.getValueOfFirst(rec, DcTerm.source, AcTerm.derivedFrom));
          m.setAudience(rec.get(DcTerm.audience));
          m.setRightsHolder(rec.get(DcTerm.rightsHolder));
          m.setCreator(rec.get(DcTerm.creator));
          m.setFormat(MEDIA_PARSER.parseMimeType(rec.get(DcTerm.format)));
          if (rec.containsKey(DcTerm.created)) {
            Range<Date> validRecordedDateRange = Range.closed(TemporalInterpreter.MIN_VALID_RECORDED_DATE, new Date());
            OccurrenceParseResult<Date> parsed = TemporalInterpreter.interpretDate(rec.get(DcTerm.created),
                    validRecordedDateRange, OccurrenceIssue.MULTIMEDIA_DATE_INVALID);
            m.setCreated(parsed.getPayload());
            occ.getIssues().addAll(parsed.getIssues());
          }

          MEDIA_PARSER.detectType(m);
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
   * Return the first multimedia Extension supported (SUPPORTED_MEDIA_EXTENSIONS).
   *
   * @param recordExtension
   * @return First media Extension found or null if not found
   */
  private static Extension getMultimediaExtension(Set<Extension> recordExtension){
    for(Extension ext: SUPPORTED_MEDIA_EXTENSIONS){
      if(recordExtension.contains(ext)){
        return ext;
      }
    }
    return null;
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
        if (!media.containsKey(url)) {
          media.put(url, m);
        } //else --> // merge infos about the same image?
      }
    }
    occ.setMedia(Lists.newArrayList(media.values()));
  }

}
