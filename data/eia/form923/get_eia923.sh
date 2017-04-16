#!/bin/sh

# This script will re-download all of the EIA923 data directly from EIA.

EIA923_XLS_URL="https://www.eia.gov/electricity/data/eia923/xls"

EIA923_ZIPS="f923_2017 \
             f923_2016 \
             f923_2015 \
             f923_2014 \
             f923_2013 \
             f923_2012 \
             f923_2011 \
             f923_2010 \
             f923_2009 \
             f923_2008 \
             f906920_2007 \
             f906920_2006 \
             f906920_2005 \
             f906920_2004 \
             f906920_2003 \
             f906920_2002 \
             f906920_2001 "

for z in $EIA923_ZIPS
do
    echo "Downloading EIA 923 data $z.zip"
    curl --progress-bar $EIA923_XLS_URL/{$z}.zip -o '#1.zip'
    mkdir -p $z
    mv $z.zip $z
    (cd $z; unzip -q $z.zip)
    # Make the data store read only for safety
    chmod -R a-w $z
done
