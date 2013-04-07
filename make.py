# encoding: utf-8
from __future__ import division

import sys, subprocess, itertools, os, os.path, shutil, json, math

import psycopg2
import argparse, operator

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

def properties(rows):
    results = {
        # weighted (by road distance) average of the ratio
        'average': None,

        ## How many metres of the roads have a ratio ≥ 1.2
        #'m_above_1_2': None,

        ## How many metres of the roads have a ratio ≥ 1.5
        #'m_above_1_5': None,

        ## What percentage of the road metres have a ratio ≥ 1.2
        #'percent_above_1_2': None,

        ## What percentage of the road metres have a ratio ≥ 1.5
        #'percent_above_1_5': None,

        ## catches dead straight and almost straight roads
        #'percent_below_1_001': None,
        #'percent_below_1_2': None,
        #'percent_below_1_5': None,

        # weighted standard deviation
        'stddev': None,
    }
    total_length = 0
    total_weighted_length = 0
    ratio_comparers = {
        'total_above_1_2': lambda ratio: ratio >= 1.2,
        'total_above_1_5': lambda ratio: ratio >= 1.5,
        'total_below_1_001': lambda ratio: ratio <= 1.001,
        'total_below_1_2': lambda ratio: ratio <= 1.2,
        'total_below_1_5': lambda ratio: ratio <= 1.5,
    }

    ratio_comparers_working = {x:0 for x in ratio_comparers}

    results.update(ratio_comparers_working)
    num_rows = 0

    for row in rows:
        num_rows += 1
        highway, ratio, length = row
        total_length += length
        total_weighted_length += length*ratio
        for ratio_cmp_name, ratio_cmp_func in ratio_comparers.items():
            if ratio_cmp_func(ratio):
                ratio_comparers_working[ratio_cmp_name] += length


    if num_rows > 0:
        results['average'] = total_weighted_length / total_length
        results.update({key.replace("total_", "percent_"): ratio_comparers_working[key]/total_length for key in ratio_comparers_working})
    else:
        results.update({key.replace("total_", "percent_"): 0 for key in ratio_comparers_working})


    results.update(ratio_comparers_working)


    mean_ratio = results['average']
    num_nonzero_weights = num_rows
    #num_nonzero_weights = sum(1 for highway, ratio, length in rows if length > 0)
    if num_rows > 0:
        stddev = math.sqrt(
            sum(length*((ratio - mean_ratio)**2) for highway, ratio, length in rows) / 
                ( ( (num_nonzero_weights + 1) / num_nonzero_weights ) * total_length )
        )

        results['stddev'] = stddev

    return results


def generate_data(minlat, maxlat, minlon, maxlon, increment):
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
                these_properties = properties(rows)

                yield {
                    'properties': these_properties,
                    "coordinates": [[
                        [this_minlat, this_minlon],
                        [this_maxlat, this_minlon],
                        [this_maxlat, this_maxlon],
                        [this_minlat, this_maxlon],
                        [this_minlat, this_minlon],
                    ]],
                    'bbox': bbox,
                    'minlat': this_minlat,
                    'minlon': this_minlon,
                    'maxlat': this_maxlat,
                    'maxlon': this_maxlon,
                }

def geojson_data(minlat, maxlat, minlon, maxlon, increment, output_prefix="output.", ):
    # initialize the geojson object
    geojson = {'type': 'FeatureCollection', 'features': [] }

    # This stores the values of the properties for each box. (i.e. a list of
    # dicts). This is to make iterating over the results easier, rather than
    # having to iterate over the geojson object
    all_property_results = []

    try:
        for box_details in generate_data(minlat=minlat, minlon=minlon, maxlon=maxlon, maxlat=maxlat, increment=increment):
            geojson_feature = {
                'properties': box_details['properties'],
                'type': 'Feature',
                "geometry": {
                    "type": "Polygon",
                    "coordinates": box_details['coordinates'],
                }
            }
            geojson['features'].append(geojson_feature)
            all_property_results.append(box_details['properties'])

    finally:

        print "\nSaving to %sgeojson.js" % output_prefix

        with open(output_prefix+"geojson.js", 'w') as output_fp:

            output_fp.write('var boxes = ')
            json.dump(geojson, output_fp, indent=1)
            output_fp.write(';')

        print "\nCalculating statistics"
        stats = {}
        # Make an empty call to properties with a dud values to get the keys
        for property_name in properties([]).keys():
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
        with open(output_prefix+"stats.geojson.js", 'w') as output_fp:
            json.dump(stats, output_fp, indent=1)


def save_to_postgres(minlat, maxlat, minlon, maxlon, increment, table_name="bendy_roads"):
    property_names = properties([]).keys()
    property_names.sort()
    
    conn2 = psycopg2.connect("dbname=gis")
    cursor = conn2.cursor()

    cursor.execute("CREATE TABLE {0} (id serial primary key, ".format(table_name)+", ".join("{0} float".format(pr) for pr in property_names)+");")
    cursor.execute("SELECT AddGeometryColumn('{0}', 'bbox', 900913, 'POLYGON', 2);".format(table_name))
    conn2.commit()

    # This stores the values of the properties for each box. (i.e. a list of
    # dicts). This is to make iterating over the results easier, rather than
    # having to iterate over the geojson object
    all_property_results = []


    try:
        for box_details in generate_data(minlat=minlat, minlon=minlon, maxlon=maxlon, maxlat=maxlat, increment=increment):
            sql_to_insert = ("INSERT INTO {0} (bbox, {1}) VALUES ("+box_details['bbox']+", {2});").format(table_name, ", ".join(property_names), ", ".join("%("+x+")s" for x in property_names))

            cursor.execute(sql_to_insert, box_details['properties'])
            all_property_results.append(box_details['properties'])

    finally:

        conn2.commit()
        cursor.close()
        conn2.close()

        print "\nCalculating statistics"
        stats = {}
        # Make an empty call to properties with a dud values to get the keys
        for property_name in property_names:
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
        with open(table_name+".stats.json", 'w') as output_fp:
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
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-i', '--inc', default=1.0, type=float)
    parser.add_argument('-t', '--top', default=89, type=float)
    parser.add_argument('-l', '--left', default=-179, type=float)
    parser.add_argument('-b', '--bottom', default=-89, type=float)
    parser.add_argument('-r', '--right', default=179, type=float)
    parser.add_argument('-o', '--output', default="output.", type=str)

    args = parser.parse_args()

    increment = args.inc
    top, bottom = args.top, args.bottom
    left, right = args.left, args.right
    minlat, maxlat = left, right
    minlon, maxlon = bottom, top
    #import_data(filename="../planet-130206-highways.osm.pbf")
    generate_data(minlat=minlat, maxlat=maxlat, minlon=minlon, maxlon=maxlon, increment=increment, output_prefix=args.output)
