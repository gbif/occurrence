# GBIF Occurrence Registry Synchronizer

This module contains the minimal code to listen for messages from the registry about anything that has changed there.

Of primary interest are datasets and organizations whose "owning organization" has changed, or if that organization's
country has changed.
