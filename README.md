Cloud9
======

A Hadoop toolkit for working with big data: http://cloud9lib.org/

### Features added in this forked project:

1) XML Input splitting: Although the current version of Cloud9 supports reading
   compressed files (.bzip2 etc.) in both local and Map Reduce setting, it does
   not support the splitting of tag blocks into individual InputSplit. Here I 
   have integrated the great code from wikihadoop 
   (https://github.com/whym/wikihadoop) into WikipediaPageInputFormat, to make
   the processing parallel without the need for repacking and decompressing the
   dump file.