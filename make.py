from __future__ import division

import sys, subprocess, itertools, os, os.path, shutil, json

import psycopg2

conn = psycopg2.connect("dbname=gis")
cur = conn.cursor()


def frange(start, stop, step=None):
    step = step or 1.0
    stop = stop or start
    cur = start
    while cur < stop:
        yield cur
        cur += step


def import_data():
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

    conn.commit()



def average(rows):
    # highway, ratio, length
    return sum(ratio*length for highway, ratio, length in rows)/sum(length for highway, ratio, length in rows)

def km_above_X(rows, x):
    # highway, ratio, length
    return sum(length for highway, ratio, length in rows if ratio >= x)

def percent_above_X(rows, x):
    # highway, ratio, length
    return sum(length for highway, ratio, length in rows if ratio >= x)/sum(length for highway, ratio, length in rows)


def generate_data(minlat, maxlat, minlon, maxlon, increment):
    geojson = {'type': 'FeatureCollection', 'features': [] }

    for lat, lon in itertools.product(frange(minlat, maxlat, increment), frange(minlon, maxlon, increment)):
        this_minlat, this_minlon = lat, lon
        this_maxlat, this_maxlon = lat + increment, lon + increment

        bbox = "ST_Transform(ST_MakeEnvelope({0}, {1}, {2}, {3}, 4326), 900913)".format(this_minlat, this_minlon, this_maxlat, this_maxlon)

        cur.execute(
            "select highway, case when straightline=0 then 0.0 else length::float/straightline::float end as ratio, length from ( select osm_id, highway, st_length(geog) as length, st_distance(geography(st_transform(st_startpoint(way), 4326)), geography(st_transform(st_endpoint(way), 4326))) as straightline from planet_osm_line where way && {bbox} ) as inter;".format(bbox=bbox)
        )

        rows = cur.fetchall()
        if len(rows) > 0:
            geojson_feature = {
                'properties': {
                    'average': average(rows),
                    'km_above_1_2': km_above_X(rows, 1.2),
                    'km_above_1_5': km_above_X(rows, 1.5),
                    'percent_above_1_2': percent_above_X(rows, 1.2),
                    'percent_above_1_5': percent_above_X(rows, 1.5),
                },
                'type': 'Feature',
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [this_minlon, this_minlat],
                        [this_maxlon, this_minlat],
                        [this_maxlon, this_maxlat],
                        [this_minlon, this_maxlat],
                        [this_minlon, this_minlat],
                    ]]
                }
            }
            geojson['features'].append(geojson_feature)


    with open("output.geojson.js", 'w') as output_fp:

        output_fp.write('var boxes = ')
        json.dump(geojson, output_fp, indent=1)
        output_fp.write(';')




if __name__ == '__main__':
    increment = 0.2
    minlat, maxlat = -10.0, -5.0
    minlon, maxlon = 51.0, 56.0
    #import_data()
    generate_data(minlat=minlat, maxlat=maxlat, minlon=minlon, maxlon=maxlon, increment=increment)
