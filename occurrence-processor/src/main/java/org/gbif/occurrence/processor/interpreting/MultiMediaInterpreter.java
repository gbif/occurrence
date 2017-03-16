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
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import static org.gbif.common.parsers.date.TemporalAccessorUtils.toDate;

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

    //the order is important since we will keep the first object that appears for each URI
    List<MediaObject> mediaList = Lists.newLinkedList();
    List<URI> mediaUri = Lists.newLinkedList();

    // handle possible multimedia extensions first
    final Extension mediaExt = getMultimediaExtension(verbatim.getExtensions().keySet());
    if (mediaExt != null) {
      for (Map<Term, String> rec : verbatim.getExtensions().get(mediaExt)) {
        //For AUDUBON, we use accessURI over identifier
        //TODO handle AUDUBON in its own method
        URI uri = UrlParser.parse(Terms.getValueOfFirst(rec, AcTerm.accessURI, DcTerm.identifier));
        URI link = UrlParser.parse(Terms.getValueOfFirst(rec, DcTerm.references, AcTerm.furtherInformationURL,
                AcTerm.attributionLinkURL));
        // link or media uri must exist
        if (uri != null || link != null) {
          MediaObject m = new MediaObject();
          m.setIdentifier(uri);
          m.setReferences(link);
          m.setTitle(rec.get(DcTerm.title));
          m.setDescription(Terms.getValueOfFirst(rec, DcTerm.description, AcTerm.caption));
          m.setLicense(Terms.getValueOfFirst(rec, DcTerm.license, DcTerm.rights));
          m.setPublisher(rec.get(DcTerm.publisher));
          m.setContributor(rec.get(DcTerm.contributor));
          m.setSource(Terms.getValueOfFirst(rec, DcTerm.source, AcTerm.derivedFrom));
          m.setAudience(rec.get(DcTerm.audience));
          m.setRightsHolder(rec.get(DcTerm.rightsHolder));
          m.setCreator(rec.get(DcTerm.creator));
          m.setFormat(MEDIA_PARSER.parseMimeType(rec.get(DcTerm.format)));
          if (rec.containsKey(DcTerm.created)) {
            Range<LocalDate> validRecordedDateRange = Range.closed(TemporalInterpreter.MIN_LOCAL_DATE, LocalDate.now());
            OccurrenceParseResult<TemporalAccessor> parsed = TemporalInterpreter.interpretLocalDate(rec.get(DcTerm.created),
                    validRecordedDateRange, OccurrenceIssue.MULTIMEDIA_DATE_INVALID);
            m.setCreated(toDate(parsed.getPayload()));
            occ.getIssues().addAll(parsed.getIssues());
          }

          MEDIA_PARSER.detectType(m);
          mediaList.add(m);
          mediaUri.add(getPreferredURI(m));
        } else {
          occ.getIssues().add(OccurrenceIssue.MULTIMEDIA_URI_INVALID);
        }
      }
    }

    // media via core term
    if (verbatim.hasVerbatimField(DwcTerm.associatedMedia)) {
      for (URI uri : UrlParser.parseUriList(verbatim.getVerbatimField(DwcTerm.associatedMedia))) {
        if (uri == null) {
          occ.getIssues().add(OccurrenceIssue.MULTIMEDIA_URI_INVALID);
        } else {
          // only try to build the object if we don't already got it from the extension
          if(!mediaUri.contains(uri)) {
            MediaObject m = new MediaObject();
            m.setIdentifier(uri);
            MEDIA_PARSER.detectType(m);
            mediaList.add(m);
          }
        }
      }
    }

    // make sure information is not given several times for the same image
    occ.setMedia(deduplicateMedia(mediaList));
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
   * We can get file uris or weblinks. Prefer file URIs as they clearly identify a single image
   * @param mediaObject
   * @return
   */
  private static URI getPreferredURI(MediaObject mediaObject){
    return mediaObject.getIdentifier() != null ? mediaObject.getIdentifier() : mediaObject.getReferences();
  }

  /**
   * Merges media records if the same image URL or link is given several times.
   * Remove any media that has not either a file or webpage uri.
   * @return a new list
   */
  private static List<MediaObject> deduplicateMedia(List<MediaObject> mediaList) {
    Map<String, MediaObject> media = Maps.newLinkedHashMap();
    for (MediaObject m : mediaList) {

      URI uri = getPreferredURI(m);
      if (uri != null) {
        String url = uri.toString();
        if (!media.containsKey(url)) {
          media.put(url, m);
        }
      }
    }
    return Lists.newArrayList(media.values());
  }

}
