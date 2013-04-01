# encoding: utf-8
from __future__ import division

import sys, subprocess, itertools, os, os.path, shutil, json, math

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


def import_data(filename):
    subprocess.call(['osm2pgsql', '--slim', '-S', 'osm.style', filename])
    cur.execute(
            """
            delete from planet_osm_line where
                highway not in ('trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'road', 'residential', 'primary_link', 'secondary_link', 'trunk_link', 'motorway', 'motorway_link') or highway IS NULL;
            """)
    print "Removed non-highways"
    cur.execute("""
            alter table planet_osm_line add column geog geography;
            update planet_osm_line set geog = geography(st_transform(way, 4326));
            alter table planet_osm_line add column length float;
            alter table planet_osm_line add straightline float;
            alter table planet_osm_line add ratio float;
            """)
    print "Added a geography column"
    cur.execute("""
            update planet_osm_line set length=st_length(geog), straightline=st_distance(geography(st_transform(st_startpoint(way), 4326)), geography(st_transform(st_endpoint(way), 4326)));
            update planet_osm_line set ratio=(case when straightline=0 then 0.0 else length::float/straightline::float end);
        """)
    print "Added columns for ratio"
    cur.execute("""
            analyse planet_osm_line;
            """)
    print "Analyzed & optimized"

    conn.commit()



def average(rows):
    """Returns the weighted average of the ratio of each of the roads in ``rows``, the weight is the length of the road"""
    return sum(ratio*length for highway, ratio, length in rows)/sum(length for highway, ratio, length in rows)

def m_above_X(rows, x):
    """Returns the total length in metres of all the roads whose ratio is ≥ x"""
    return sum(length for highway, ratio, length in rows if ratio >= x)

def percent_above_X(rows, x):
    """Returns what percentage (technically a number between 0.0 and 1.0) of all the road distance whose ratio is greater than x. Basically a weighted percentage of all the roads whose ratio is ≥ x"""
    return sum(length for highway, ratio, length in rows if ratio >= x)/sum(length for highway, ratio, length in rows)

def percent_below_X(rows, x):
    """Returns what percentage (technically a number between 0.0 and 1.0) of all the road distance whose ratio is less than x. Basically a weighted percentage of all the roads whose ratio is ≤ x"""
    return sum(length for highway, ratio, length in rows if ratio <= x)/sum(length for highway, ratio, length in rows)

def stddev(rows):
    """Weighted (by length) stddev of ratios of the roads"""
    # formula from http://stats.stackexchange.com/a/6536/7551
    mean_ratio = sum(ratio*length for highway, ratio, length in rows)/sum(length for highway, ratio, length in rows)
    num_nonzero_weights = sum(1 for highway, ratio, length in rows if length > 0)
    return math.sqrt(
        sum(length*((ratio - mean_ratio)**2) for highway, ratio, length in rows) / 
            ( ( (num_nonzero_weights + 1) / num_nonzero_weights ) * sum(length for highway, ratio, length in rows) )
    )


def generate_data(minlat, maxlat, minlon, maxlon, increment):
    # initialize the geojson object
    geojson = {'type': 'FeatureCollection', 'features': [] }

    # For each box these are the properties we want to store for it
    property_funcs = {
        # weighted (by road distance) average of the ratio
        'average': lambda rows: average(rows),

        # How many metres of the roads have a ratio ≥ 1.2
        'm_above_1_2': lambda rows: m_above_X(rows, 1.2),

        # How many metres of the roads have a ratio ≥ 1.5
        'm_above_1_5': lambda rows: m_above_X(rows, 1.5),

        # What percentage of the road metres have a ratio ≥ 1.2
        'percent_above_1_2': lambda rows: percent_above_X(rows, 1.2),

        # What percentage of the road metres have a ratio ≥ 1.5
        'percent_above_1_5': lambda rows: percent_above_X(rows, 1.5),

        # catches dead straight and almost straight roads
        'percent_below_1_001': lambda rows: percent_below_X(rows, 1.001),
        'percent_below_1_2': lambda rows: percent_below_X(rows, 1.2),
        'percent_below_1_5': lambda rows: percent_below_X(rows, 1.5),

        # weighted standard deviation
        'stddev': lambda rows: stddev(rows),
    }

    # This stores the values of the properties for each box. (i.e. a list of
    # dicts). This is to make iterating over the results easier, rather than
    # having to iterate over the geojson object
    all_property_results = []

    try:
        for lat in frange(minlat, maxlat, increment):
            percent = ((lat - minlat) / (maxlat - minlat) ) * 100
            sys.stdout.write("\n[%3d%%] %s " % (percent, lat))

            for lon in frange(minlon, maxlon, increment):
                sys.stdout.write(".")

                this_minlat, this_minlon = lat, lon
                this_maxlat, this_maxlon = lat + increment, lon + increment

                # postgis bbox of this box
                bbox = "ST_Transform(ST_MakeEnvelope({0}, {1}, {2}, {3}, 4326), 900913)".format(this_minlat, this_minlon, this_maxlat, this_maxlon)

                cur.execute(
                    "select highway, case when straightline=0 then 0.0 else length::float/straightline::float end as ratio, length from ( select osm_id, highway, st_length(geog) as length, st_distance(geography(st_transform(st_startpoint(way), 4326)), geography(st_transform(st_endpoint(way), 4326))) as straightline from planet_osm_line where way && {bbox} ) as inter;".format(bbox=bbox)
                )

                rows = cur.fetchall()
                if len(rows) > 0:
                    properties = {k: v(rows) for k, v in property_funcs.items()}
                    geojson_feature = {
                        'properties': properties,
                        'type': 'Feature',
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[
                                [this_minlat, this_minlon],
                                [this_maxlat, this_minlon],
                                [this_maxlat, this_maxlon],
                                [this_minlat, this_maxlon],
                                [this_minlat, this_minlon],
                            ]]
                        }
                    }
                    geojson['features'].append(geojson_feature)
                    all_property_results.append(properties)

    finally:

        print "\nSaving to output.geojson.js"

        with open("output.geojson.js", 'w') as output_fp:

            output_fp.write('var boxes = ')
            json.dump(geojson, output_fp, indent=1)
            output_fp.write(';')

        print "\nCalculating statistics"
        stats = {}
        for property_name in property_funcs.keys():
            values = [x[property_name] for x in all_property_results]
            values.sort()
            if len(values) == 0:
                continue
            mean = sum(values) / len(values)
            stats[property_name] = {
                'avg': mean,
                'min': values[0],
                'max': values[-1],
                'median': values[int(len(values)/2)],
                'p10': values[int(len(values)*0.1)],
                'p90': values[int(len(values)*0.9)],
                'p25': values[int(len(values)*0.25)],
                'p75': values[int(len(values)*0.75)],
                'stddev': math.sqrt(sum((i - mean) ** 2 for i in values) / len(values)),
            }
        with open("output.stats.geojson.js", 'w') as output_fp:
            json.dump(stats, output_fp, indent=1)



def extract_way_details(minlat, maxlat, minlon, maxlon, increment):
    results = {}
    try:
        for lat in frange(minlat, maxlat, increment):
            percent = ((lat - minlat) / (maxlat - minlat) ) * 100
            sys.stdout.write("\n[%3d%%] %s " % (percent, lat))
            for lon in frange(minlon, maxlon, increment):
                sys.stdout.write(".")
                this_minlat, this_minlon = lat, lon
                this_maxlat, this_maxlon = lat + increment, lon + increment

                bbox = "ST_Transform(ST_MakeEnvelope({0}, {1}, {2}, {3}, 4326), 900913)".format(this_minlat, this_minlon, this_maxlat, this_maxlon)

                cur.execute(
                    "select case when straightline=0 then 0.0 else length::float/straightline::float end as ratio, length from ( select osm_id, highway, st_length(geog) as length, st_distance(geography(st_transform(st_startpoint(way), 4326)), geography(st_transform(st_endpoint(way), 4326))) as straightline from planet_osm_line where way && {bbox} ) as inter;".format(bbox=bbox)
                )

                rows = cur.fetchall()
                if len(rows) > 0:
                    if lat not in results:
                        results[lat] = {}
                    results[lat][lon] = {
                        'minlat': this_minlat,
                        'maxlat': this_maxlat,
                        'minlat': this_minlat,
                        'minlon': this_minlon,
                        'rows': rows,
                    }

    finally:

        print "\nSaving to way_details.json"

        with open("way_details.json", 'w') as output_fp:
            json.dump(results, output_fp, indent=1)




if __name__ == '__main__':
    increment = 1.0
    top, bottom = 89, -89
    left, right = -179, 179
    minlat, maxlat = left, right
    minlon, maxlon = bottom, top
    #import_data(filename="../planet-130206-highways.osm.pbf")
    generate_data(minlat=minlat, maxlat=maxlat, minlon=minlon, maxlon=maxlon, increment=increment)
