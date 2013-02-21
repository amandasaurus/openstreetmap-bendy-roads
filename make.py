from __future__ import division

import sys, subprocess, itertools

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

for lat, lon in itertools.product(frange(minlat, maxlat, increment), frange(minlon, maxlon, increment)):
    print "{0} {1}".format(lat, lon)
    this_minlat, this_minlon = lat, lon
    this_maxlat, this_maxlon = lat + increment, lon + increment

    subprocess.call(['osm2pgsql', '-S', 'osm.style', 'ireland-and-northern-ireland.osm.pbf']
    subprocess.call(['psql', '-d', 'gis', '-c', """
            delete from planet_osm_line where
                highway not in ('trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'road', 'residential', 'primary_link', 'secondary_link', 'trunk_link', 'motorway', 'motorway_link') or highway IS NULL;
            alter table planet_osm_line add column geog geography;
            update planet_osm_line set geog = geography(st_transform(way, 4326));
            """])
    print "Dumping data"
    subprocess.call(["psql", "-d", "gis", "-t", "-A", "-F", "	", "-o", "output", "-c", "select highway, case when straightline=0 then 0.0 else length::float/straightline::float end as ratio, length, straightline from ( select osm_id, highway, st_length(geog) as length, st_distance(geography(st_transform(st_startpoint(way), 4326)), geography(st_transform(st_endpoint(way), 4326))) as straightline from planet_osm_line ) as inter;"])

# delete from planet_osm_line where highway not in ('trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'road', 'residential', 'primary_link', 'secondary_link', 'trunk_link', 'motorway', 'motorway_link') or highway IS NULL;
# alter table planet_osm_line add column geog geography;
# update planet_osm_line set geog = geography(st_transform(way, 4326));
# select osm_id, case when straightline=0 then 0.0 else length::float/straightline::float end as ratio, length, straightline from ( select osm_id, highway, st_length(geog) as length, st_distance(geography(st_transform(st_startpoint(way), 4326)), geography(st_transform(st_endpoint(way), 4326))) as straightline from planet_osm_line ) as inter
