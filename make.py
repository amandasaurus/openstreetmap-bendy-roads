from __future__ import division

import sys, subprocess, itertools, os, os.path, shutil

import psycopg2

conn = psycopg2.connect("dbname=gis")
cur = conn.cursor()

increment = 0.2

def frange(start, stop, step=None):
    step = step or 1.0
    stop = stop or start
    cur = start
    while cur < stop:
        yield cur
        cur += step

minlat, maxlat = -10.0, -5.0
minlon, maxlon = 51.0, 56.0

if os.path.isdir("output"):
    shutil.rmtree("output")
os.mkdir("./output")

subprocess.call(['osm2pgsql', '-S', 'osm.style', 'ireland-and-northern-ireland.osm.pbf'])
cur.execute(
        """
        delete from planet_osm_line where
            highway not in ('trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'road', 'residential', 'primary_link', 'secondary_link', 'trunk_link', 'motorway', 'motorway_link') or highway IS NULL;
        """)
print "Removed non-highways"
cur.execute("""
        alter table planet_osm_line add column geog geography;
        update planet_osm_line set geog = geography(st_transform(way, 4326));
        """)
print "Added a geography column"
cur.execute("""
        analyse planet_osm_line;
        """)
print "Analyzed & optimized"
        #create index planet_osm_line_geog on planet_osm_line using gist (geog);

for lat, lon in itertools.product(frange(minlat, maxlat, increment), frange(minlon, maxlon, increment)):
    this_minlat, this_minlon = lat, lon
    this_maxlat, this_maxlon = lat + increment, lon + increment

    bbox = "ST_Transform(ST_MakeEnvelope({0}, {1}, {2}, {3}, 4326), 900913)".format(this_minlat, this_minlon, this_maxlat, this_maxlon)

    filename= "output.minlat{0}.maxlat{1}.minlon{2}.maxlon{3}.tsv".format(this_minlat, this_maxlat, this_minlon, this_maxlon)
    subprocess.call([
        "psql", "-d", "gis", "-t", "-A", "-F", "	",
        "-o", filename,
        "-c",
        "select highway, case when straightline=0 then 0.0 else length::float/straightline::float end as ratio, length, straightline from ( select osm_id, highway, st_length(geog) as length, st_distance(geography(st_transform(st_startpoint(way), 4326)), geography(st_transform(st_endpoint(way), 4326))) as straightline from planet_osm_line where way && {bbox} ) as inter;".format(bbox=bbox),
        ])

    if os.path.getsize(filename) == 0:
        os.remove(filename)
    else:
        print "Saved to ", filename
